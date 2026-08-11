-- Optional reconciliation helper. This function is not installed by the
-- subscription package. Copy it into your own migrations and adapt it as needed.

-- Reconcile the complete desired subscription set for one stream. Rows that did
-- not change are left untouched so their database triggers are not rebuilt.
CREATE OR REPLACE FUNCTION pgstream_subscriptions.set_subscriptions(
    p_stream_id bigint,
    p_subscriptions pgstream_subscriptions.subscriptions[]
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
begin
    merge into pgstream_subscriptions.subscriptions as target
    using (
        select desired.*
        from unnest(p_subscriptions) as desired
    ) as source
    on target.key = source.key and target.stream_id = p_stream_id
    when matched and (
        target.operation is distinct from source.operation or
        target.schema_name is distinct from source.schema_name or
        target.table_name is distinct from source.table_name or
        target.when_clause is distinct from source.when_clause or
        target.column_names is distinct from source.column_names or
        target.metadata is distinct from source.metadata or
        target.payload_extensions is distinct from source.payload_extensions or
        target.metadata_extensions is distinct from source.metadata_extensions
    ) then update set
        operation = source.operation,
        schema_name = source.schema_name,
        table_name = source.table_name,
        when_clause = source.when_clause,
        column_names = source.column_names,
        metadata = source.metadata,
        payload_extensions = source.payload_extensions,
        metadata_extensions = source.metadata_extensions
    when not matched then insert (
        key,
        stream_id,
        operation,
        schema_name,
        table_name,
        when_clause,
        column_names,
        metadata,
        payload_extensions,
        metadata_extensions
    ) values (
        source.key,
        p_stream_id,
        source.operation,
        source.schema_name,
        source.table_name,
        source.when_clause,
        source.column_names,
        source.metadata,
        source.payload_extensions,
        source.metadata_extensions
    );

    delete from pgstream_subscriptions.subscriptions as existing
    where existing.stream_id = p_stream_id
      and not exists (
          select 1
          from unnest(p_subscriptions) as desired
          where desired.key = existing.key
      );
end;
$$;
