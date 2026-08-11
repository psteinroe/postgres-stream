-- Optional pgstream subscription package.
--
-- Copy this migration into your database migration system and adjust schema,
-- ownership, and grants for your environment before applying it. Run it as a
-- role that owns every table that subscriptions may target.

CREATE SCHEMA pgstream_subscriptions;

create type pgstream_subscriptions.operation_type as enum (
    'INSERT',
    'UPDATE',
    'DELETE'
);

create table if not exists pgstream_subscriptions.subscriptions (
    id uuid primary key default pgstream.portable_uuidv7(),
    key text not null,
    stream_id bigint,
    operation pgstream_subscriptions.operation_type not null,
    schema_name text not null,
    table_name text not null,
    when_clause text,
    column_names text[] not null,
    metadata jsonb,
    payload_extensions jsonb default '[]'::jsonb,
    metadata_extensions jsonb default '[]'::jsonb,
    unique (stream_id, key, schema_name, table_name, operation)
);

CREATE OR REPLACE FUNCTION pgstream_subscriptions.build_extensions(p_extensions jsonb)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $$
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

CREATE OR REPLACE FUNCTION pgstream_subscriptions.sync_database_trigger() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    AS $_$
declare
    v_table_name text := coalesce(new.table_name, old.table_name);
    v_schema_name text := coalesce(new.schema_name, old.schema_name);
    v_when_clause text;
    v_if_blocks text;
    v_op pgstream_subscriptions.operation_type;
begin
    foreach v_op in array array['INSERT', 'UPDATE', 'DELETE']::pgstream_subscriptions.operation_type[] loop
        execute format(
            $sql$drop trigger if exists pgstream_%s on %I.%I;$sql$,
            lower(v_op::text), v_schema_name, v_table_name
        );

        execute format(
            $sql$drop function if exists pgstream_subscriptions._publish_after_%s_on_%s;$sql$,
            lower(v_op::text), v_table_name
        );

        if exists (select 1 from pgstream_subscriptions.subscriptions where table_name = v_table_name and schema_name = v_schema_name and operation = v_op) then
            -- if there is at least one subscription for v_op operation without a when_clause or with an empty one, we do not add the when clause at all
            v_when_clause := (
                case when exists (
                    select 1
                    from pgstream_subscriptions.subscriptions
                    where table_name = v_table_name and schema_name = v_schema_name and operation = v_op and (when_clause is null or when_clause = '')
                ) then null
                else (
                    select string_agg(when_clause, ') or (')
                    from pgstream_subscriptions.subscriptions
                    where table_name = v_table_name and schema_name = v_schema_name and operation = v_op and when_clause is not null and when_clause != ''
                )
                end
            );

            -- Build if blocks that collect both payload and metadata per subscription
            v_if_blocks := (
                select string_agg(format(
                    $sql$
                    if %s then
                        v_payloads := array_append(v_payloads, jsonb_build_object(
                            'tg_name', %L,
                            'new', case when tg_op is distinct from 'DELETE' then jsonb_build_object(
                                %s
                            ) else null end,
                            'old', case when tg_op is distinct from 'INSERT' then jsonb_build_object(
                                %s
                            ) else null end
                        ) || v_base_payload || (%s));
                        v_metadatas := array_append(v_metadatas, coalesce(%L::jsonb, '{}'::jsonb) || (%s));
                    end if;
                    $sql$,
                    coalesce(nullif(subscription.when_clause, ''), 'true'),
                    subscription.key,
                    (select string_agg(format($s$%L, new.%I$s$, column_name, column_name), ', ') from unnest(subscription.column_names) as column_name),
                    (select string_agg(format($s$%L, old.%I$s$, column_name, column_name), ', ') from unnest(subscription.column_names) as column_name),
                    pgstream_subscriptions.build_extensions(subscription.payload_extensions),
                    subscription.metadata,
                    pgstream_subscriptions.build_extensions(subscription.metadata_extensions)
                ), e'\n') from pgstream_subscriptions.subscriptions as subscription where table_name = v_table_name and schema_name = v_schema_name and operation = v_op
            );

            execute format(
                $sql$
                create or replace function pgstream_subscriptions._publish_after_%s_on_%s ()
                    returns trigger
                    as $inner$
                declare
                    v_payloads jsonb[] := '{}';
                    v_metadatas jsonb[] := '{}';

                    v_base_payload jsonb := jsonb_build_object(
                        'tg_op', tg_op,
                        'tg_table_name', tg_table_name,
                        'tg_table_schema', tg_table_schema,
                        'timestamp', (extract(epoch from now()) * 1000)::bigint
                    );
                begin
                    %s

                    if array_length(v_payloads, 1) > 0 then
                        insert into pgstream.events (payload, metadata, stream_id, lsn)
                        select p, m, %L, pg_current_wal_lsn()
                        from unnest(v_payloads, v_metadatas) as t(p, m);
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
                (select distinct stream_id from pgstream_subscriptions.subscriptions where table_name = v_table_name and schema_name = v_schema_name and operation = v_op limit 1)
            );

            execute format(
                $sql$
                    create constraint trigger pgstream_%s
                    after %s on %I.%I
                    deferrable initially deferred
                    for each row
                    %s
                    execute procedure pgstream_subscriptions._publish_after_%s_on_%s()
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

CREATE TRIGGER sync_database_trigger
AFTER INSERT OR DELETE OR UPDATE ON pgstream_subscriptions.subscriptions
FOR EACH ROW EXECUTE FUNCTION pgstream_subscriptions.sync_database_trigger();
