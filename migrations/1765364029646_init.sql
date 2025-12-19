create schema if not exists pgstream;

create or replace function pgstream.portable_uuidv7()
 returns uuid
 language plpgsql
as $function$
declare
  v_server_num integer := current_setting('server_version_num')::int;
  ts_ms bigint;
  b bytea;
  rnd bytea;
  i int;
begin
  if v_server_num >= 180000 then
    return uuidv7 ();
  end if;
  ts_ms := floor(extract(epoch from clock_timestamp()) * 1000)::bigint;
  rnd := uuid_send(gen_random_uuid());
  b := repeat(e'\\000', 16)::bytea;
  for i in 0..5 loop
    b := set_byte(b, i, ((ts_ms >> ((5 - i) * 8)) & 255)::int);
  end loop;
  for i in 6..15 loop
    b := set_byte(b, i, get_byte(rnd, i));
  end loop;
  b := set_byte(b, 6, ((get_byte(b, 6) & 15) | (7 << 4)));
  b := set_byte(b, 8, ((get_byte(b, 8) & 63) | 128));
  return encode(b, 'hex')::uuid;
end;
$function$;

-- Events table for durable streaming
create table if not exists pgstream.events (
    id uuid default pgstream.portable_uuidv7() not null,
    payload jsonb not null,
    metadata jsonb,
    stream_id bigint not null,
    created_at timestamptz default clock_timestamp() not null,
    primary key (created_at, id)
) partition by range (created_at);

-- Stream state table for tracking stream health and maintenance
create table if not exists pgstream.streams (
    id bigint primary key,
    next_maintenance_at timestamptz not null default now(),
    failover_checkpoint_id text,
    failover_checkpoint_ts timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);


create or replace function pgstream.sync_publications() returns trigger
    language plpgsql security definer
    as $_$
begin
    if tg_op = 'INSERT' then
        execute format(
            $pub$create publication %I for table pgstream.events where (stream_id = %L) with (publish_via_partition_root = true, publish = 'insert');$pub$,
            'pgstream_stream_' || new.id, new.id
        );
    elsif tg_op = 'DELETE' then
        execute format(
            'drop publication if exists %I',
            'pgstream_stream_' || new.id
        );
        return old;
    end if;

    return new;
end
$_$;

create or replace trigger sync_publications
after insert or delete on pgstream.streams
for each row execute function pgstream.sync_publications();

create type pgstream.operation_type as enum (
    'INSERT',
    'UPDATE',
    'DELETE'
);

create table if not exists pgstream.subscriptions (
    id uuid primary key default pgstream.portable_uuidv7(),
    key text not null,
    stream_id bigint,
    operation pgstream.operation_type not null,
    schema_name text not null,
    table_name text not null,
    when_clause text,
    column_names text[] not null,
    metadata jsonb,
    payload_extensions jsonb default '[]'::jsonb,
    unique (stream_id, key, schema_name, table_name, operation)
);

create or replace function pgstream.build_payload_from_extensions(p_extensions jsonb)
returns text
language plpgsql
stable
as $$
declare
    rec record;
    path_parts text[];
    top_key text;
    grouped_paths jsonb := '{}'::jsonb;
    result_parts text[];
    nested_items text[];
    nested_expr text;
    i int;
begin
    -- Group extensions by top-level key
    for rec in
        select
            value->>'json_path' as json_path,
            value->>'expression' as expression
        from jsonb_array_elements(p_extensions)
        order by value->>'json_path'
    loop
        path_parts := string_to_array(rec.json_path, '.');
        top_key := path_parts[1];

        -- Add to grouped paths
        if not grouped_paths ? top_key then
            grouped_paths := jsonb_set(grouped_paths, array[top_key], '[]'::jsonb);
        end if;

        grouped_paths := jsonb_set(
            grouped_paths,
            array[top_key],
            (grouped_paths->top_key) || jsonb_build_array(
                jsonb_build_object('parts', to_jsonb(path_parts), 'expr', rec.expression)
            )
        );
    end loop;

    -- Build result for each top-level key
    for rec in select key, value from jsonb_each(grouped_paths) order by key loop
        top_key := rec.key;
        nested_items := '{}';

        -- Process all items for this top-level key
        for nested_expr, path_parts in
            select
                item->>'expr',
                array(select jsonb_array_elements_text(item->'parts'))
            from jsonb_array_elements(rec.value) item
        loop
            if array_length(path_parts, 1) = 1 then
                -- Top-level only, no nesting
                result_parts := array_append(result_parts, format('%L, %s', top_key, nested_expr));
                nested_items := null;
                exit;
            else
                -- Build nested structure from second element onward
                nested_expr := nested_expr;
                for i in reverse array_length(path_parts, 1) .. 2 loop
                    nested_expr := format('jsonb_build_object(%L, %s)', path_parts[i], nested_expr);
                end loop;
                nested_items := array_append(nested_items, nested_expr);
            end if;
        end loop;

        -- Combine nested items if any
        if nested_items is not null and array_length(nested_items, 1) > 0 then
            if array_length(nested_items, 1) = 1 then
                result_parts := array_append(result_parts, format('%L, %s', top_key, nested_items[1]));
            else
                result_parts := array_append(result_parts, format('%L, (%s)', top_key, array_to_string(nested_items, ' || ')));
            end if;
        end if;
    end loop;

    if array_length(result_parts, 1) > 0 then
        return 'jsonb_build_object(' || array_to_string(result_parts, ', ') || ')';
    else
        return '''{}''::jsonb';
    end if;
end;
$$;

create or replace function pgstream.sync_database_trigger() returns trigger
    language plpgsql security definer
    as $_$
declare
    v_table_name text := coalesce(new.table_name, old.table_name);
    v_schema_name text := coalesce(new.schema_name, old.schema_name);
    v_when_clause text;
    v_if_blocks text;
    v_op pgstream.operation_type;
begin
    foreach v_op in array array['INSERT', 'UPDATE', 'DELETE']::pgstream.operation_type[] loop
        execute format(
            $sql$drop trigger if exists pgstream_%s on %I.%I;$sql$,
            lower(v_op::text), v_schema_name, v_table_name
        );

        execute format(
            $sql$drop function if exists pgstream._publish_after_%s_on_%s;$sql$,
            lower(v_op::text), v_table_name
        );

        if exists (select 1 from pgstream.subscriptions where table_name = v_table_name and schema_name = v_schema_name and operation = v_op) then
            -- if there is at least one subscription for v_op operation without a when_clause or with an empty one, we do not add the when clause at all
            v_when_clause := (
                case when exists (
                    select 1
                    from pgstream.subscriptions
                    where table_name = v_table_name and schema_name = v_schema_name and operation = v_op and (when_clause is null or when_clause = '')
                ) then null
                else (
                    select string_agg(when_clause, ') or (')
                    from pgstream.subscriptions
                    where table_name = v_table_name and schema_name = v_schema_name and operation = v_op and when_clause is not null and when_clause != ''
                )
                end
            );

            v_if_blocks := (
                select string_agg(format(
                    $sql$
                    if %s then
                        v_jsonb_output := v_jsonb_output || (jsonb_build_object(
                            'tg_name', %L,
                            'new', case when tg_op is distinct from 'DELETE' then jsonb_build_object(
                                %s
                            ) else null end,
                            'old', case when tg_op is distinct from 'INSERT' then jsonb_build_object(
                                %s
                            ) else null end
                        ) || v_base_payload || (%s));
                    end if;
                    $sql$,
                    coalesce(nullif(subscription.when_clause, ''), 'true'),
                    subscription.key,
                    (select string_agg(format($s$%L, new.%I$s$, column_name, column_name), ', ') from unnest(subscription.column_names) as column_name),
                    (select string_agg(format($s$%L, old.%I$s$, column_name, column_name), ', ') from unnest(subscription.column_names) as column_name),
                    pgstream.build_payload_from_extensions(subscription.payload_extensions)
                ), e'\n') from pgstream.subscriptions as subscription where table_name = v_table_name and schema_name = v_schema_name and operation = v_op
            );

            execute format(
                $sql$
                create or replace function pgstream._publish_after_%s_on_%s ()
                    returns trigger
                    as $inner$
                declare
                    v_jsonb_output jsonb := '[]'::jsonb;

                    v_base_payload jsonb := jsonb_build_object(
                        'tg_op', tg_op,
                        'tg_table_name', tg_table_name,
                        'tg_table_schema', tg_table_schema,
                        'timestamp', (extract(epoch from now()) * 1000)::bigint
                    );
                begin
                    %s

                    if jsonb_array_length(v_jsonb_output) > 0 then
                        insert into pgstream.events (payload, stream_id)
                        select elem, %L
                        from jsonb_array_elements(v_jsonb_output) as t(elem);
                    end if;

                    if tg_op = 'DELETE' then
                        return old;
                    end if;

                    return new;
                end
                $inner$
                language plpgsql
                set search_path = ''
                security definer;
                $sql$,
                lower(v_op::text),
                v_table_name,
                v_if_blocks,
                (select distinct stream_id from pgstream.subscriptions where table_name = v_table_name and schema_name = v_schema_name and operation = v_op limit 1)
            );

            execute format(
                $sql$
                    create constraint trigger pgstream_%s
                    after %s on %I.%I
                    deferrable initially deferred
                    for each row
                    %s
                    execute procedure pgstream._publish_after_%s_on_%s()
                $sql$,
                lower(v_op::text),
                lower(v_op::text),
                v_schema_name,
                v_table_name,
                case when v_when_clause is not null and length(v_when_clause) > 0
                     then 'when ((' || v_when_clause || '))'
                     else ''
                end,
                lower(v_op::text),
                v_table_name
            );
        end if;
    end loop;

    if tg_op = 'DELETE' then
        return old;
    end if;

    return new;
end
$_$;

create or replace trigger sync_database_trigger
after insert or delete or update on pgstream.subscriptions
for each row execute function pgstream.sync_database_trigger();

