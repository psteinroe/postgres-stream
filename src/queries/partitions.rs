use sqlx::{PgPool, prelude::FromRow};

#[derive(FromRow)]
pub struct PartitionRecord {
    pub partition_name: String,
}

/// Lists all partitions for a table by schema and table name.
pub async fn list_partitions(
    pool: &PgPool,
    schema_name: &str,
    table_name: &str,
) -> sqlx::Result<Vec<PartitionRecord>> {
    sqlx::query_as(
        r#"
        select c.relname as partition_name
        from pg_class c
        join pg_inherits i on c.oid = i.inhrelid
        join pg_class p on i.inhparent = p.oid
        join pg_namespace n on p.relnamespace = n.oid
        where n.nspname = $1
          and p.relname = $2
        order by c.relname
        "#,
    )
    .bind(schema_name)
    .bind(table_name)
    .fetch_all(pool)
    .await
}

/// Creates a partition with explicit range bounds.
pub async fn create_partition(
    pool: &PgPool,
    schema_name: &str,
    table_name: &str,
    partition_name: &str,
    range_start: &str,
    range_end: &str,
) -> sqlx::Result<()> {
    let qualified_partition = format!("{schema_name}.{partition_name}");
    let qualified_table = format!("{schema_name}.{table_name}");

    sqlx::query(&format!(
        r#"
        create table if not exists {qualified_partition}
        partition of {qualified_table}
        for values from ('{range_start}') to ('{range_end}')
        "#
    ))
    .execute(pool)
    .await?;

    Ok(())
}

/// Drops a partition by schema and name.
pub async fn drop_partition(
    pool: &PgPool,
    schema_name: &str,
    partition_name: &str,
) -> sqlx::Result<()> {
    let qualified_name = format!("{schema_name}.{partition_name}");
    sqlx::query(&format!("drop table if exists {qualified_name}"))
        .execute(pool)
        .await?;

    Ok(())
}
