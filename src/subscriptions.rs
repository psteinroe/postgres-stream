// subscriptions are manages by the database so this is just a test module for now
#[cfg(test)]
#[allow(clippy::indexing_slicing)] // Test assertions on known JSON structure
mod tests {
    use sqlx::{PgPool, Row};

    use crate::test_utils::TestDatabase;

    async fn create_test_table(pool: &PgPool) {
        sqlx::query(
            r#"
            create table public.users (
                id serial primary key,
                name text not null,
                email text not null,
                age integer,
                created_at timestamptz default now()
            )
            "#,
        )
        .execute(pool)
        .await
        .expect("Failed to create test table");
    }

    async fn create_subscription(
        pool: &PgPool,
        key: &str,
        stream_id: i64,
        operation: &str,
        when_clause: Option<&str>,
        payload_extensions: Option<serde_json::Value>,
    ) -> sqlx::types::Uuid {
        let row = sqlx::query(
            r#"
            insert into pgstream.subscriptions
                (key, stream_id, operation, schema_name, table_name, when_clause, column_names, payload_extensions)
            values ($1, $2, $3::pgstream.operation_type, 'public', 'users', $4, array['id', 'name', 'email', 'age'], $5)
            returning id
            "#,
        )
        .bind(key)
        .bind(stream_id)
        .bind(operation)
        .bind(when_clause)
        .bind(payload_extensions.unwrap_or(serde_json::json!([])))
        .fetch_one(pool)
        .await
        .expect("Failed to create subscription");

        row.get("id")
    }

    async fn get_events(pool: &PgPool, stream_id: i64) -> Vec<serde_json::Value> {
        let rows = sqlx::query(
            r#"
            select payload
            from pgstream.events
            where stream_id = $1
            order by created_at, id
            "#,
        )
        .bind(stream_id)
        .fetch_all(pool)
        .await
        .expect("Failed to fetch events");

        rows.into_iter()
            .map(|row| row.get::<serde_json::Value, _>("payload"))
            .collect()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_subscription_creates_triggers_and_events() {
        let db = TestDatabase::spawn().await;
        db.ensure_today_partition().await;

        // Create test table
        create_test_table(&db.pool).await;

        // Create subscription for INSERT operations
        create_subscription(&db.pool, "user_created", 1, "INSERT", None, None).await;

        // Insert a record
        sqlx::query(
            r#"
            insert into public.users (name, email, age)
            values ('Alice', 'alice@example.com', 30)
            "#,
        )
        .execute(&db.pool)
        .await
        .expect("Failed to insert user");

        // Verify event was created
        let events = get_events(&db.pool, 1).await;
        assert_eq!(events.len(), 1);

        let event = events.first().expect("Should have 1 event");
        assert_eq!(event["tg_name"], "user_created");
        assert_eq!(event["tg_op"], "INSERT");
        assert_eq!(event["tg_table_name"], "users");
        assert_eq!(event["tg_table_schema"], "public");
        assert_eq!(event["new"]["name"], "Alice");
        assert_eq!(event["new"]["email"], "alice@example.com");
        assert_eq!(event["new"]["age"], 30);
        assert!(event["old"].is_null());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_subscription_with_when_clause() {
        let db = TestDatabase::spawn().await;
        db.ensure_today_partition().await;

        create_test_table(&db.pool).await;

        // Create subscription with when clause for adults only
        create_subscription(
            &db.pool,
            "adult_user_created",
            2,
            "INSERT",
            Some("new.age >= 18"),
            None,
        )
        .await;

        // Insert an adult user (should trigger)
        sqlx::query(
            r#"
            insert into public.users (name, email, age)
            values ('Bob', 'bob@example.com', 25)
            "#,
        )
        .execute(&db.pool)
        .await
        .expect("Failed to insert user");

        // Insert a minor (should NOT trigger)
        sqlx::query(
            r#"
            insert into public.users (name, email, age)
            values ('Charlie', 'charlie@example.com', 15)
            "#,
        )
        .execute(&db.pool)
        .await
        .expect("Failed to insert user");

        // Verify only one event was created (for Bob)
        let events = get_events(&db.pool, 2).await;
        assert_eq!(events.len(), 1);
        assert_eq!(
            events.first().expect("Should have 1 event")["new"]["name"],
            "Bob"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_operation() {
        let db = TestDatabase::spawn().await;
        db.ensure_today_partition().await;

        create_test_table(&db.pool).await;

        // Create subscription for UPDATE operations
        create_subscription(&db.pool, "user_updated", 3, "UPDATE", None, None).await;

        // Insert a user first
        sqlx::query(
            r#"
            insert into public.users (name, email, age)
            values ('Diana', 'diana@example.com', 28)
            "#,
        )
        .execute(&db.pool)
        .await
        .expect("Failed to insert user");

        // Update the user
        sqlx::query(
            r#"
            update public.users
            set age = 29, email = 'diana.new@example.com'
            where name = 'Diana'
            "#,
        )
        .execute(&db.pool)
        .await
        .expect("Failed to update user");

        // Verify event was created
        let events = get_events(&db.pool, 3).await;
        assert_eq!(events.len(), 1);

        let event = events.first().expect("Should have 1 event");
        assert_eq!(event["tg_op"], "UPDATE");
        assert_eq!(event["old"]["age"], 28);
        assert_eq!(event["old"]["email"], "diana@example.com");
        assert_eq!(event["new"]["age"], 29);
        assert_eq!(event["new"]["email"], "diana.new@example.com");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_delete_operation() {
        let db = TestDatabase::spawn().await;
        db.ensure_today_partition().await;

        create_test_table(&db.pool).await;

        // Create subscription for DELETE operations
        create_subscription(&db.pool, "user_deleted", 4, "DELETE", None, None).await;

        // Insert a user first
        sqlx::query(
            r#"
            insert into public.users (name, email, age)
            values ('Eve', 'eve@example.com', 35)
            "#,
        )
        .execute(&db.pool)
        .await
        .expect("Failed to insert user");

        // Delete the user
        sqlx::query(
            r#"
            delete from public.users
            where name = 'Eve'
            "#,
        )
        .execute(&db.pool)
        .await
        .expect("Failed to delete user");

        // Verify event was created
        let events = get_events(&db.pool, 4).await;
        assert_eq!(events.len(), 1);

        let event = events.first().expect("Should have 1 event");
        assert_eq!(event["tg_op"], "DELETE");
        assert_eq!(event["old"]["name"], "Eve");
        assert_eq!(event["old"]["email"], "eve@example.com");
        assert!(event["new"].is_null());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_payload_extensions() {
        let db = TestDatabase::spawn().await;
        db.ensure_today_partition().await;

        create_test_table(&db.pool).await;

        // Create subscription with payload extensions
        // Note: expressions are SQL expressions evaluated at trigger execution time
        // String literals need quotes, column references don't (e.g., new.message_id)
        let payload_extensions = serde_json::json!([
            {"json_path": "metadata.source", "expression": "'web_app'"},
            {"json_path": "metadata.version", "expression": "'1.0.0'"},
            {"json_path": "custom_field", "expression": "'custom_value'"}
        ]);

        create_subscription(
            &db.pool,
            "user_with_extensions",
            5,
            "INSERT",
            None,
            Some(payload_extensions.clone()),
        )
        .await;

        // Debug: Check what build_payload_from_extensions returns
        let payload_expr: String =
            sqlx::query_scalar("select pgstream.build_payload_from_extensions($1)")
                .bind(&payload_extensions)
                .fetch_one(&db.pool)
                .await
                .expect("Failed to get build_payload_from_extensions result");
        println!("build_payload_from_extensions SQL expression: {payload_expr}");

        // Debug: Check what the generated function looks like
        let func_def: String = sqlx::query_scalar(
            "select pg_get_functiondef('pgstream._publish_after_insert_on_users'::regproc)",
        )
        .fetch_one(&db.pool)
        .await
        .expect("Failed to get function definition");
        println!("Generated function:\n{func_def}");

        // Insert a user
        sqlx::query(
            r#"
            insert into public.users (name, email, age)
            values ('Frank', 'frank@example.com', 40)
            "#,
        )
        .execute(&db.pool)
        .await
        .expect("Failed to insert user");

        // Verify event has payload extensions
        let events = get_events(&db.pool, 5).await;
        assert_eq!(events.len(), 1);

        let event = events.first().expect("Should have 1 event");
        println!(
            "Event payload: {}",
            serde_json::to_string_pretty(event).unwrap()
        );
        assert_eq!(event["new"]["name"], "Frank");
        assert_eq!(event["metadata"]["source"], "web_app");
        assert_eq!(event["metadata"]["version"], "1.0.0");
        assert_eq!(event["custom_field"], "custom_value");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multiple_subscriptions_same_stream() {
        let db = TestDatabase::spawn().await;
        db.ensure_today_partition().await;

        create_test_table(&db.pool).await;

        // Create two subscriptions on the same stream with different when clauses
        create_subscription(
            &db.pool,
            "young_user",
            6,
            "INSERT",
            Some("new.age < 30"),
            None,
        )
        .await;
        create_subscription(
            &db.pool,
            "old_user",
            6,
            "INSERT",
            Some("new.age >= 30"),
            None,
        )
        .await;

        // Insert a young user
        sqlx::query(
            r#"
            insert into public.users (name, email, age)
            values ('Grace', 'grace@example.com', 25)
            "#,
        )
        .execute(&db.pool)
        .await
        .expect("Failed to insert user");

        // Insert an older user
        sqlx::query(
            r#"
            insert into public.users (name, email, age)
            values ('Henry', 'henry@example.com', 35)
            "#,
        )
        .execute(&db.pool)
        .await
        .expect("Failed to insert user");

        // Verify both events were created
        let events = get_events(&db.pool, 6).await;
        assert_eq!(events.len(), 2);

        // First event should be for Grace (young_user)
        let first_event = events.first().expect("Should have first event");
        assert_eq!(first_event["tg_name"], "young_user");
        assert_eq!(first_event["new"]["name"], "Grace");

        // Second event should be for Henry (old_user)
        let second_event = events.get(1).expect("Should have second event");
        assert_eq!(second_event["tg_name"], "old_user");
        assert_eq!(second_event["new"]["name"], "Henry");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_subscription_deletion_removes_trigger() {
        let db = TestDatabase::spawn().await;
        db.ensure_today_partition().await;

        create_test_table(&db.pool).await;

        // Create subscription
        let subscription_id =
            create_subscription(&db.pool, "temp_subscription", 7, "INSERT", None, None).await;

        // Insert a user (should create event)
        sqlx::query(
            r#"
            insert into public.users (name, email, age)
            values ('Ivy', 'ivy@example.com', 22)
            "#,
        )
        .execute(&db.pool)
        .await
        .expect("Failed to insert user");

        // Verify event was created
        let events_before = get_events(&db.pool, 7).await;
        assert_eq!(events_before.len(), 1);

        // Delete the subscription
        sqlx::query(
            r#"
            delete from pgstream.subscriptions
            where id = $1
            "#,
        )
        .bind(subscription_id)
        .execute(&db.pool)
        .await
        .expect("Failed to delete subscription");

        // Insert another user (should NOT create event)
        sqlx::query(
            r#"
            insert into public.users (name, email, age)
            values ('Jack', 'jack@example.com', 27)
            "#,
        )
        .execute(&db.pool)
        .await
        .expect("Failed to insert user");

        // Verify no new event was created
        let events_after = get_events(&db.pool, 7).await;
        assert_eq!(events_after.len(), 1); // Still just the first event
    }
}
