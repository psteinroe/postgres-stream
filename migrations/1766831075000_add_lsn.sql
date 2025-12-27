-- add lsn column to events table for precise slot recovery
-- the lsn is captured at trigger execution time to enable exact replay point lookup
-- when a replication slot is invalidated

alter table pgstream.events
add column if not exists lsn pg_lsn;

-- update the sync_database_trigger function to capture lsn on insert
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
                        insert into pgstream.events (payload, stream_id, lsn)
                        select elem, %L, pg_current_wal_lsn()
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

