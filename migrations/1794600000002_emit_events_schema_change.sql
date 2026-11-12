-- Announce schema changes of pgstream.events to the running pipelines.
--
-- etl keeps its own copy of the events table schema and decodes every row
-- against it. It learns about schema changes from transactional
-- `supabase_etl_ddl` logical messages, which etl's source migrations emit from
-- a `ddl_command_end` event trigger. pgstream cannot install that trigger
-- without superuser, so a migration that alters pgstream.events must emit the
-- message itself by calling this function in the same transaction, after the
-- `alter table`:
--
--     alter table pgstream.events add column foo text;
--     select pgstream.emit_events_schema_change();
--
-- Without it, the next replicated row carries columns etl does not know and
-- the pipeline fails with `CorruptedTableSchema` on every restart.
--
-- The payload has the shape etl's `etl.emit_schema_change_messages()` emits
-- (supabase/etl d5098c27, crates/etl/src/postgres/codec/event.rs). The
-- message is emitted for the partitioned root table only: the publication
-- uses `publish_via_partition_root`, so etl only sees the root relation.
--
-- `pg_logical_emit_message` is executable by every role, so this works
-- without superuser or REPLICATION. If etl's event trigger is installed too,
-- etl receives two identical schemas for the same transaction, which is
-- harmless.

create or replace function pgstream.emit_events_schema_change()
returns void
language plpgsql
set search_path = pg_catalog
as
$fnc$
declare
    v_table pg_catalog.oid := 'pgstream.events'::pg_catalog.regclass;
    v_columns pg_catalog.jsonb;
begin
    -- Without logical WAL there is no downstream consumer for the message.
    if pg_catalog.current_setting('wal_level', true) is distinct from 'logical' then
        return;
    end if;

    select pg_catalog.jsonb_agg(
        pg_catalog.jsonb_build_object(
            'attname', s.attname,
            'attnum', s.attnum,
            'atttypid', s.atttypid::pg_catalog.int8,
            'typname', s.typname,
            'formatted_type', s.formatted_type,
            'atttypmod', s.atttypmod,
            'attnotnull', s.attnotnull,
            'atthasdef', s.atthasdef,
            'default_expression', s.default_expression,
            'attidentity', s.attidentity,
            'atthasmissing', s.atthasmissing
        )
        order by s.attnum
    )
    into v_columns
    from etl.describe_table_schema(v_table) s;

    perform pg_catalog.pg_logical_emit_message(
        true,
        'supabase_etl_ddl',
        pg_catalog.convert_to(
            pg_catalog.jsonb_build_object(
                'trigger_event', 'ddl_command_end',
                'command_tag', 'ALTER TABLE',
                'current_query', pg_catalog.current_query(),
                'current_database', pg_catalog.current_database(),
                'server_version_num', pg_catalog.current_setting('server_version_num')::pg_catalog.int4,
                'nspname', 'pgstream',
                'relname', 'events',
                'oid', v_table::pg_catalog.int8,
                'relkind', (select c.relkind::pg_catalog.text from pg_catalog.pg_class c where c.oid = v_table),
                'commands', '[]'::pg_catalog.jsonb,
                'identity', etl.describe_table_identity(v_table),
                'columns', v_columns
            )::pg_catalog.text,
            'utf8'
        )
    );
end;
$fnc$;

-- Only the roles that may alter pgstream.events (its owner, superusers) need it.
revoke all on function pgstream.emit_events_schema_change() from public;

comment on function pgstream.emit_events_schema_change() is
$$Emits the `supabase_etl_ddl` logical message etl needs to pick up a schema
change of pgstream.events. Call it in the same transaction as, and after, every
`alter table pgstream.events`.$$;
