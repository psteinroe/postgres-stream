-- Add the partitioned BRIN index used to look up events by WAL LSN.
--
-- A new database has no event partitions yet, so creating the parent index is
-- safe and all partitions created later will inherit a child index. Existing
-- databases must install the partitioned index online before this migration.
DO $$
DECLARE
    parent_index_oid oid;
    parent_index_is_expected boolean;
    has_leaf_partitions boolean;
BEGIN
    SELECT
        index_relation.oid,
        coalesce(
            index_relation.relkind = 'I'
            AND access_method.amname = 'brin'
            AND index_metadata.indrelid = 'pgstream.events'::regclass
            AND index_metadata.indisready
            AND index_metadata.indisvalid
            AND index_metadata.indnatts = 1
            AND index_metadata.indkey[0] = lsn_attribute.attnum
            AND index_metadata.indpred IS NULL
            AND index_metadata.indexprs IS NULL,
            false
        )
    INTO parent_index_oid, parent_index_is_expected
    FROM pg_class AS index_relation
    JOIN pg_namespace AS index_namespace
      ON index_namespace.oid = index_relation.relnamespace
    LEFT JOIN pg_am AS access_method
      ON access_method.oid = index_relation.relam
    LEFT JOIN pg_index AS index_metadata
      ON index_metadata.indexrelid = index_relation.oid
    LEFT JOIN pg_attribute AS lsn_attribute
      ON lsn_attribute.attrelid = 'pgstream.events'::regclass
     AND lsn_attribute.attname = 'lsn'
     AND NOT lsn_attribute.attisdropped
    WHERE index_namespace.nspname = 'pgstream'
      AND index_relation.relname = 'events_lsn_brin_idx';

    IF parent_index_oid IS NOT NULL THEN
        IF NOT parent_index_is_expected THEN
            RAISE EXCEPTION
                'pgstream.events_lsn_brin_idx exists but is not a ready, valid, non-partial partitioned BRIN index on pgstream.events(lsn); inspect or remove it before retrying the migration';
        END IF;

        -- Production databases that completed the online rollout already have
        -- the expected index.
        RETURN;
    END IF;

    -- Prevent maintenance from adding a partition between the safety check and
    -- CREATE INDEX. Existing databases take the online concurrent rollout path.
    LOCK TABLE pgstream.events IN SHARE MODE NOWAIT;

    SELECT EXISTS (
        SELECT 1
        FROM pg_partition_tree('pgstream.events'::regclass) AS partition_tree
        WHERE partition_tree.isleaf
    )
    INTO has_leaf_partitions;

    IF has_leaf_partitions THEN
        RAISE EXCEPTION
            'pgstream.events has existing leaf partitions but pgstream.events_lsn_brin_idx is absent; install and attach the BRIN index online before retrying the migration';
    END IF;

    CREATE INDEX events_lsn_brin_idx
        ON pgstream.events
        USING brin (lsn)
        WITH (pages_per_range = 64, autosummarize = on);
END
$$;
