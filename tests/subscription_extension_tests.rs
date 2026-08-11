use postgres_stream::test_utils::TestDatabase;
use sqlx::Row;

const LEGACY_INIT: &str = include_str!("../migrations/1765364029646_init.sql");
const LEGACY_LSN: &str = include_str!("../migrations/1766831075000_add_lsn.sql");
const LEGACY_METADATA: &str =
    include_str!("../migrations/1767795878880_add_metadata_extensions.sql");
const EXTRACT_SUBSCRIPTIONS: &str =
    include_str!("../migrations/1786434463000_extract_subscriptions.sql");
const INSTALL_SUBSCRIPTIONS: &str =
    include_str!("../extensions/subscriptions/migrations/0001_create_pgstream_subscriptions.sql");
const SET_SUBSCRIPTIONS_EXAMPLE: &str =
    include_str!("../extensions/subscriptions/examples/set_subscriptions.sql");
const UPGRADE_SUBSCRIPTIONS: &str =
    include_str!("../extensions/subscriptions/upgrades/from-pgstream-0.1.sql");
const UNINSTALL_SUBSCRIPTIONS: &str = include_str!("../extensions/subscriptions/uninstall.sql");

async fn create_users_table(database: &TestDatabase) {
    sqlx::query(
        r#"
        create table public.users (
            id bigint generated always as identity primary key,
            email text not null
        )
        "#,
    )
    .execute(&database.pool)
    .await
    .expect("Failed to create users table");
}

async fn set_user_subscription(database: &TestDatabase) {
    sqlx::query(
        r#"
        select pgstream_subscriptions.set_subscriptions(
            1,
            array[
                row(
                    null::uuid,
                    'user-created',
                    1::bigint,
                    'INSERT'::pgstream_subscriptions.operation_type,
                    'public',
                    'users',
                    null::text,
                    array['id', 'email']::text[],
                    null::jsonb,
                    '[]'::jsonb,
                    '[]'::jsonb
                )::pgstream_subscriptions.subscriptions
            ]
        )
        "#,
    )
    .execute(&database.pool)
    .await
    .expect("Failed to reconcile subscriptions");
}

async fn insert_trigger_oid(database: &TestDatabase) -> i64 {
    sqlx::query_scalar(
        r#"
        select trigger.oid::bigint
        from pg_catalog.pg_trigger as trigger
        join pg_catalog.pg_class as relation on relation.oid = trigger.tgrelid
        join pg_catalog.pg_namespace as namespace on namespace.oid = relation.relnamespace
        where namespace.nspname = 'public'
          and relation.relname = 'users'
          and trigger.tgname = 'pgstream_insert'
        "#,
    )
    .fetch_one(&database.pool)
    .await
    .expect("Failed to find subscription trigger")
}

#[tokio::test(flavor = "multi_thread")]
async fn core_migrations_do_not_install_subscriptions() {
    let database = TestDatabase::spawn().await;

    let row = sqlx::query(
        r#"
        select
            to_regclass('pgstream.subscriptions')::text as legacy,
            to_regclass('pgstream_subscriptions.subscriptions')::text as extracted
        "#,
    )
    .fetch_one(&database.pool)
    .await
    .expect("Failed to inspect subscription schemas");

    assert_eq!(row.get::<Option<String>, _>("legacy"), None);
    assert_eq!(row.get::<Option<String>, _>("extracted"), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn optional_set_subscriptions_reconciles_and_skips_unchanged_definitions() {
    let database = TestDatabase::spawn_with_subscriptions().await;
    create_users_table(&database).await;

    let installed_by_default: bool = sqlx::query_scalar(
        "select to_regprocedure('pgstream_subscriptions.set_subscriptions(bigint, pgstream_subscriptions.subscriptions[])') is not null",
    )
    .fetch_one(&database.pool)
    .await
    .expect("Failed to inspect optional helper");
    assert!(!installed_by_default);

    sqlx::raw_sql(SET_SUBSCRIPTIONS_EXAMPLE)
        .execute(&database.pool)
        .await
        .expect("Failed to install optional set_subscriptions example");
    set_user_subscription(&database).await;
    let initial_trigger_oid = insert_trigger_oid(&database).await;

    set_user_subscription(&database).await;
    let unchanged_trigger_oid = insert_trigger_oid(&database).await;
    assert_eq!(initial_trigger_oid, unchanged_trigger_oid);

    sqlx::query(
        r#"
        select pgstream_subscriptions.set_subscriptions(
            1,
            array[]::pgstream_subscriptions.subscriptions[]
        )
        "#,
    )
    .execute(&database.pool)
    .await
    .expect("Failed to remove subscriptions");

    let trigger_exists: bool = sqlx::query_scalar(
        r#"
        select exists (
            select 1
            from pg_catalog.pg_trigger as trigger
            join pg_catalog.pg_class as relation on relation.oid = trigger.tgrelid
            join pg_catalog.pg_namespace as namespace on namespace.oid = relation.relnamespace
            where namespace.nspname = 'public'
              and relation.relname = 'users'
              and trigger.tgname = 'pgstream_insert'
        )
        "#,
    )
    .fetch_one(&database.pool)
    .await
    .expect("Failed to inspect removed trigger");

    assert!(!trigger_exists);
}

#[tokio::test(flavor = "multi_thread")]
async fn core_migration_blocks_nonempty_legacy_subscriptions() {
    let database = TestDatabase::spawn_without_migrations().await;

    for migration in [LEGACY_INIT, LEGACY_LSN, LEGACY_METADATA] {
        sqlx::raw_sql(migration)
            .execute(&database.pool)
            .await
            .expect("Failed to install legacy pgstream schema");
    }
    create_users_table(&database).await;

    sqlx::query(
        r#"
        insert into pgstream.subscriptions (
            key, stream_id, operation, schema_name, table_name, column_names
        ) values (
            'user-created', 1, 'INSERT', 'public', 'users', array['id', 'email']
        )
        "#,
    )
    .execute(&database.pool)
    .await
    .expect("Failed to create legacy subscription");

    let error = sqlx::raw_sql(EXTRACT_SUBSCRIPTIONS)
        .execute(&database.pool)
        .await
        .expect_err("Core migration should block legacy subscriptions");
    assert!(
        error
            .to_string()
            .contains("legacy pgstream subscriptions must be upgraded")
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from pgstream.subscriptions")
            .fetch_one(&database.pool)
            .await
            .expect("Failed to verify preserved legacy subscriptions"),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn package_owner_manages_triggers_without_runtime_table_privileges() {
    let database = TestDatabase::spawn().await;
    database.ensure_today_partition().await;

    sqlx::query("create role app_owner nologin")
        .execute(&database.pool)
        .await
        .expect("Failed to create application owner");
    sqlx::query("create role pgstream_runtime nologin")
        .execute(&database.pool)
        .await
        .expect("Failed to create pgstream runtime role");

    let database_name = database.config.name.replace('"', "\"\"");
    sqlx::query(&format!(
        "grant create on database \"{database_name}\" to app_owner"
    ))
    .execute(&database.pool)
    .await
    .expect("Failed to grant schema creation");
    sqlx::query("grant create on schema public to app_owner")
        .execute(&database.pool)
        .await
        .expect("Failed to grant application schema creation");
    sqlx::query("grant usage on schema pgstream to app_owner")
        .execute(&database.pool)
        .await
        .expect("Failed to grant core schema usage");
    sqlx::query("grant insert on pgstream.events to app_owner")
        .execute(&database.pool)
        .await
        .expect("Failed to grant event insertion");

    let mut connection = database.pool.acquire().await.expect("Failed to connect");
    sqlx::query("set role app_owner")
        .execute(&mut *connection)
        .await
        .expect("Failed to assume application owner role");
    sqlx::query(
        "create table public.users (id bigint generated always as identity primary key, email text not null)",
    )
    .execute(&mut *connection)
    .await
    .expect("Application owner failed to create target table");
    sqlx::raw_sql(INSTALL_SUBSCRIPTIONS)
        .execute(&mut *connection)
        .await
        .expect("Application owner failed to install subscriptions");
    sqlx::query(
        r#"
        insert into pgstream_subscriptions.subscriptions (
            key, stream_id, operation, schema_name, table_name, column_names
        ) values (
            'user-created', 1, 'INSERT', 'public', 'users', array['id', 'email']
        )
        "#,
    )
    .execute(&mut *connection)
    .await
    .expect("Application owner failed to create subscription");
    sqlx::query("insert into public.users (email) values ('owner@example.com')")
        .execute(&mut *connection)
        .await
        .expect("Subscription trigger failed under application owner");
    sqlx::query("reset role")
        .execute(&mut *connection)
        .await
        .expect("Failed to reset role");
    drop(connection);

    let runtime_has_trigger: bool = sqlx::query_scalar(
        "select has_table_privilege('pgstream_runtime', 'public.users', 'TRIGGER')",
    )
    .fetch_one(&database.pool)
    .await
    .expect("Failed to inspect runtime privileges");
    assert!(!runtime_has_trigger);

    let target_owner: String = sqlx::query_scalar(
        "select pg_get_userbyid(relowner) from pg_class where oid = 'public.users'::regclass",
    )
    .fetch_one(&database.pool)
    .await
    .expect("Failed to inspect target owner");
    assert_eq!(target_owner, "app_owner");

    let event_name: String = sqlx::query_scalar("select payload->>'tg_name' from pgstream.events")
        .fetch_one(&database.pool)
        .await
        .expect("Subscription did not emit an event");
    assert_eq!(event_name, "user-created");
}

#[tokio::test(flavor = "multi_thread")]
async fn uninstall_removes_package_and_managed_triggers() {
    let database = TestDatabase::spawn_with_subscriptions().await;
    create_users_table(&database).await;
    sqlx::raw_sql(SET_SUBSCRIPTIONS_EXAMPLE)
        .execute(&database.pool)
        .await
        .expect("Failed to install optional set_subscriptions example");
    set_user_subscription(&database).await;

    sqlx::raw_sql(UNINSTALL_SUBSCRIPTIONS)
        .execute(&database.pool)
        .await
        .expect("Failed to uninstall subscription package");

    let package_exists: bool = sqlx::query_scalar(
        "select exists(select 1 from pg_namespace where nspname = 'pgstream_subscriptions')",
    )
    .fetch_one(&database.pool)
    .await
    .expect("Failed to inspect package schema");
    assert!(!package_exists);

    let trigger_exists: bool = sqlx::query_scalar(
        "select exists(select 1 from pg_trigger where tgrelid = 'public.users'::regclass and tgname = 'pgstream_insert')",
    )
    .fetch_one(&database.pool)
    .await
    .expect("Failed to inspect managed trigger");
    assert!(!trigger_exists);
}

#[tokio::test(flavor = "multi_thread")]
async fn upgrade_moves_subscriptions_without_recreating_rows_or_triggers() {
    let database = TestDatabase::spawn_without_migrations().await;

    for migration in [LEGACY_INIT, LEGACY_LSN, LEGACY_METADATA] {
        sqlx::raw_sql(migration)
            .execute(&database.pool)
            .await
            .expect("Failed to install legacy pgstream schema");
    }

    database.ensure_today_partition().await;
    create_users_table(&database).await;

    let subscription_id: sqlx::types::Uuid = sqlx::query_scalar(
        r#"
        insert into pgstream.subscriptions (
            key,
            stream_id,
            operation,
            schema_name,
            table_name,
            column_names,
            payload_extensions,
            metadata_extensions
        ) values (
            'user-created',
            1,
            'INSERT',
            'public',
            'users',
            array['id', 'email'],
            '[]',
            '[]'
        )
        returning id
        "#,
    )
    .fetch_one(&database.pool)
    .await
    .expect("Failed to create legacy subscription");

    let trigger_oid = insert_trigger_oid(&database).await;
    let trigger_function_oid: i64 =
        sqlx::query_scalar("select tgfoid::bigint from pg_trigger where oid::bigint = $1")
            .bind(trigger_oid)
            .fetch_one(&database.pool)
            .await
            .expect("Failed to find legacy trigger function");

    sqlx::raw_sql(UPGRADE_SUBSCRIPTIONS)
        .execute(&database.pool)
        .await
        .expect("Failed to upgrade subscription package");

    let moved_subscription_id: sqlx::types::Uuid =
        sqlx::query_scalar("select id from pgstream_subscriptions.subscriptions")
            .fetch_one(&database.pool)
            .await
            .expect("Failed to find moved subscription");
    assert_eq!(subscription_id, moved_subscription_id);
    assert_eq!(trigger_oid, insert_trigger_oid(&database).await);

    let moved_function = sqlx::query(
        r#"
        select procedure.oid::bigint as oid, namespace.nspname as schema_name
        from pg_catalog.pg_proc as procedure
        join pg_catalog.pg_namespace as namespace on namespace.oid = procedure.pronamespace
        where procedure.oid::bigint = $1
        "#,
    )
    .bind(trigger_function_oid)
    .fetch_one(&database.pool)
    .await
    .expect("Failed to inspect moved trigger function");
    assert_eq!(moved_function.get::<i64, _>("oid"), trigger_function_oid);
    assert_eq!(
        moved_function.get::<String, _>("schema_name"),
        "pgstream_subscriptions"
    );

    // The core extraction migration must accept an already-upgraded installation.
    sqlx::raw_sql(EXTRACT_SUBSCRIPTIONS)
        .execute(&database.pool)
        .await
        .expect("Core extraction migration removed the user-managed package");

    sqlx::query("insert into public.users (email) values ('upgrade@example.com')")
        .execute(&database.pool)
        .await
        .expect("Moved trigger failed");

    let event_name: String = sqlx::query_scalar("select payload->>'tg_name' from pgstream.events")
        .fetch_one(&database.pool)
        .await
        .expect("Moved trigger did not emit an event");
    assert_eq!(event_name, "user-created");
}
