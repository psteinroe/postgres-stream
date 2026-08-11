-- Upgrade subscriptions created by pgstream 0.1.x in place.
--
-- Copy and review this migration before running it. It must run as a role that
-- can administer both the legacy pgstream objects and every subscribed table.
-- ALTER ... SET SCHEMA preserves subscription rows, object OIDs, and trigger
-- dependencies; target-table triggers are not recreated by this migration.

BEGIN;

DO $$
BEGIN
    IF to_regclass('pgstream.subscriptions') IS NULL THEN
        RAISE EXCEPTION 'pgstream.subscriptions does not exist; use the fresh-install migration instead';
    END IF;

    IF to_regclass('pgstream_subscriptions.subscriptions') IS NOT NULL THEN
        RAISE EXCEPTION 'pgstream_subscriptions.subscriptions already exists';
    END IF;
END;
$$;

LOCK TABLE pgstream.subscriptions IN ACCESS EXCLUSIVE MODE;

CREATE SCHEMA pgstream_subscriptions AUTHORIZATION CURRENT_USER;

-- Move generated functions without recreating them. Target triggers reference
-- these functions by OID, so their dependencies remain valid.
DO $$
DECLARE
    v_function_oid oid;
    v_signature text;
BEGIN
    FOR v_function_oid IN
        SELECT DISTINCT procedure.oid
        FROM pg_catalog.pg_trigger AS trigger
        JOIN pg_catalog.pg_proc AS procedure ON procedure.oid = trigger.tgfoid
        JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
        WHERE NOT trigger.tgisinternal
          AND trigger.tgname IN ('pgstream_insert', 'pgstream_update', 'pgstream_delete')
          AND namespace.nspname = 'pgstream'
          AND procedure.proname LIKE '\_publish\_after\_%' ESCAPE '\'
    LOOP
        SELECT v_function_oid::regprocedure::text INTO v_signature;
        EXECUTE format('ALTER FUNCTION %s SET SCHEMA pgstream_subscriptions', v_signature);

        SELECT v_function_oid::regprocedure::text INTO v_signature;
        EXECUTE format('ALTER FUNCTION %s OWNER TO CURRENT_USER', v_signature);
    END LOOP;
END;
$$;

ALTER TYPE pgstream.operation_type SET SCHEMA pgstream_subscriptions;
ALTER TYPE pgstream_subscriptions.operation_type OWNER TO CURRENT_USER;

ALTER TABLE pgstream.subscriptions SET SCHEMA pgstream_subscriptions;
ALTER TABLE pgstream_subscriptions.subscriptions OWNER TO CURRENT_USER;

ALTER FUNCTION pgstream.build_extensions(jsonb) SET SCHEMA pgstream_subscriptions;
ALTER FUNCTION pgstream_subscriptions.build_extensions(jsonb) OWNER TO CURRENT_USER;

ALTER FUNCTION pgstream.sync_database_trigger() SET SCHEMA pgstream_subscriptions;
ALTER FUNCTION pgstream_subscriptions.sync_database_trigger() OWNER TO CURRENT_USER;

-- Replace only the coordinator body so future rebuilds use the new schema.
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

COMMIT;
