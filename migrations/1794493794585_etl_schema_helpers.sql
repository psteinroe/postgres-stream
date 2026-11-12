-- Schema helper functions etl calls at runtime.
--
-- etl's `Pipeline::start` normally installs these through its "source
-- migrations", together with the `supabase_etl_ddl_message_trigger` event
-- trigger. Creating an event trigger requires superuser, which pgstream does
-- not have on managed Postgres, so pgstream runs etl with
-- `run_source_migrations = false` and installs only the helpers here.
--
-- Without the trigger no `supabase_etl_ddl` schema-change messages are
-- emitted; etl only consumes those messages when they exist.
--
-- Copied verbatim from supabase/etl d5098c27
-- (crates/etl/migrations/source):
-- - etl.describe_table_schema from 20260415100000_schema_change_messages
-- - etl.describe_table_identity from 20261002120000_primary_key_deferrability
--
-- `create or replace` keeps this compatible with databases where etl's own
-- source migrations were applied by a superuser.

create schema if not exists etl;

create or replace function etl.describe_table_schema(
    p_table pg_catalog.oid
) returns table (
    attname pg_catalog.text,
    attnum pg_catalog.int4,
    atttypid pg_catalog.oid,
    typname pg_catalog.text,
    formatted_type pg_catalog.text,
    atttypmod pg_catalog.int4,
    attnotnull pg_catalog.bool,
    atthasdef pg_catalog.bool,
    default_expression pg_catalog.text,
    attidentity pg_catalog.text,
    atthasmissing pg_catalog.bool
)
language sql
stable
strict
set search_path = pg_catalog
as
$fnc$
select
    a.attname::pg_catalog.text,
    a.attnum::pg_catalog.int4,
    a.atttypid,
    t.typname::pg_catalog.text,
    pg_catalog.format_type(a.atttypid, a.atttypmod)::pg_catalog.text,
    a.atttypmod::pg_catalog.int4,
    a.attnotnull,
    a.atthasdef,
    case
        when a.atthasdef then pg_catalog.pg_get_expr(ad.adbin, ad.adrelid)::pg_catalog.text
        else null
    end,
    nullif(a.attidentity, '')::pg_catalog.text,
    a.atthasmissing
from pg_catalog.pg_attribute a
join pg_catalog.pg_type t
  on t.oid = a.atttypid
left join pg_catalog.pg_attrdef ad
  on ad.adrelid = a.attrelid
 and ad.adnum = a.attnum
where a.attrelid = p_table
  and a.attnum > 0
  and not a.attisdropped
  and a.attgenerated = ''
order by a.attnum;
$fnc$;

revoke all on function etl.describe_table_schema(pg_catalog.oid) from public;

comment on function etl.describe_table_schema(pg_catalog.oid) is
$$Returns the visible, non-generated column snapshot for one table after DDL has
been applied.

The result stays close to PostgreSQL catalog naming so the emitted JSON can act
as a source-native snapshot rather than an ETL-specific schema representation.$$;

create or replace function etl.describe_table_identity(
    p_table pg_catalog.oid
) returns pg_catalog.jsonb
language sql
stable
strict
set search_path = pg_catalog
as
$fnc$
with rel as (
    select c.relreplident
    from pg_catalog.pg_class c
    where c.oid = p_table
),
direct_parent as (
    select i.inhparent as parent_oid
    from pg_catalog.pg_inherits i
    where i.inhrelid = p_table
    order by i.inhseqno
    limit 1
),
primary_key_cols as (
    select
        x.attnum::pg_catalog.int4 as attnum,
        x.n::pg_catalog.int4 as position
    from pg_catalog.pg_constraint con
    cross join lateral unnest(con.conkey) with ordinality as x(attnum, n)
    where con.conrelid = p_table
      and con.contype = 'p'
),
parent_primary_key_cols as (
    select
        x.attnum::pg_catalog.int4 as attnum,
        x.n::pg_catalog.int4 as position
    from direct_parent dp
    join pg_catalog.pg_constraint con
      on con.conrelid = dp.parent_oid
     and con.contype = 'p'
    cross join lateral unnest(con.conkey) with ordinality as x(attnum, n)
),
effective_primary_key_cols as (
    select
        pkc.attnum,
        pkc.position
    from primary_key_cols pkc
    union all
    select
        ppkc.attnum,
        ppkc.position
    from parent_primary_key_cols ppkc
    where not exists (
        select 1
        from primary_key_cols pkc
    )
),
-- Uses the same leaf-or-parent fallback as `effective_primary_key_cols`.
effective_primary_key_condeferrable as (
    select con.condeferrable
    from pg_catalog.pg_constraint con
    where con.conrelid = p_table
      and con.contype = 'p'
    union all
    select con.condeferrable
    from direct_parent dp
    join pg_catalog.pg_constraint con
      on con.conrelid = dp.parent_oid
     and con.contype = 'p'
    where not exists (
        select 1
        from primary_key_cols pkc
    )
),
replica_identity_index as (
    select ic.relname as index_name
    from pg_catalog.pg_index i
    join pg_catalog.pg_class ic
      on ic.oid = i.indexrelid
    where i.indrelid = p_table
      and i.indisreplident
),
replica_identity_cols as (
    select
        x.attnum::pg_catalog.int4 as attnum,
        x.n::pg_catalog.int4 as position
    from pg_catalog.pg_index i
    cross join lateral unnest(i.indkey) with ordinality as x(attnum, n)
    where i.indrelid = p_table
      and i.indisreplident
      and x.n <= i.indnkeyatts
      and x.attnum > 0
)
select pg_catalog.jsonb_build_object(
    'primary_key_attnums',
    coalesce(
        (
            select pg_catalog.jsonb_agg(epkc.attnum order by epkc.position)
            from effective_primary_key_cols epkc
        ),
        '[]'::pg_catalog.jsonb
    ),
    'primary_key_condeferrable',
    coalesce(
        (
            select pg_catalog.bool_or(epkd.condeferrable)
            from effective_primary_key_condeferrable epkd
        ),
        false
    ),
    'relreplident',
    r.relreplident::pg_catalog.text,
    'replica_identity_index_relname',
    (
        select rii.index_name
        from replica_identity_index rii
        limit 1
    ),
    'replica_identity_index_attnums',
    coalesce(
        (
            select pg_catalog.jsonb_agg(ric.attnum order by ric.position)
            from replica_identity_cols ric
        ),
        '[]'::pg_catalog.jsonb
    )
)
from rel r;
$fnc$;

revoke all on function etl.describe_table_identity(pg_catalog.oid) from public;

comment on function etl.describe_table_identity(pg_catalog.oid) is
$$Returns compact identity metadata for one table, including primary-key attnums,
whether that primary key is deferrable, and replica-identity state.

This helper intentionally avoids duplicating per-column identity annotations in
the payload. For partitions, it falls back to the direct parent primary key when
the leaf table does not expose a local primary-key constraint.$$;
