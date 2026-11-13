//! A Postgres primary with a physical standby, for failover and switchover tests.
//!
//! Unlike the shared container in [`crate::test_utils::ensure_postgres`], every pair is started
//! for a single test and removed when it is dropped. The containers share a Docker network so the
//! standby can stream from the primary by container name.

use std::time::Duration;

use etl::config::{IntoConnectOptions, PgConnectionConfig, TcpKeepaliveConfig, TlsConfig};
use sqlx::{AssertSqlSafe, Connection, Executor, PgConnection, PgPool, postgres::PgPoolOptions};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

use crate::migrations::{migrate_etl, migrate_pgstream};
use crate::test_utils::{create_today_partition, test_postgres_version};

/// Name of the physical slot the standby streams from.
pub const STANDBY_SLOT_NAME: &str = "pgstream_test_standby";

/// Database created on the primary for the test. It reaches the standby through physical
/// replication.
const DATABASE_NAME: &str = "pgstream_failover";

/// Allows the standby to open a physical replication connection from another container.
const PRIMARY_INIT_SCRIPT: &str =
    r#"echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf""#;

/// How long to wait for the standby to replay WAL or to sync a slot.
const SYNC_TIMEOUT: Duration = Duration::from_secs(90);

/// A primary and a physical standby that streams from it through a physical slot.
///
/// With `sync_slots`, the standby is configured to synchronize failover slots
/// (`sync_replication_slots`, `hot_standby_feedback`, `primary_slot_name` and a `dbname` in
/// `primary_conninfo`), and the standby's slot is listed in `synchronized_standby_slots` on the
/// primary, as Postgres requires for failover slots to survive a promotion.
///
/// Requires Postgres 17+. Both containers are removed on drop.
pub struct StandbyPair {
    primary: ContainerAsync<Postgres>,
    /// Held so the standby is removed with the pair.
    _standby: ContainerAsync<GenericImage>,
    primary_port: u16,
    standby_port: u16,
}

impl StandbyPair {
    /// Returns whether the test Postgres version can sync failover slots to a standby.
    #[must_use]
    pub fn is_supported() -> bool {
        test_postgres_version() >= 17
    }

    /// Starts a primary, takes a base backup of it and starts a standby from the backup.
    ///
    /// Creates a database on the primary with etl and pgstream migrations applied.
    pub async fn start(sync_slots: bool) -> Self {
        let version = test_postgres_version();
        let suffix = Uuid::new_v4().simple().to_string();
        let network = format!("pgstream-failover-{suffix}");
        let primary_host = format!("pgstream-primary-{suffix}");

        let primary = Postgres::default()
            .with_tag(format!("{version}-alpine"))
            .with_cmd([
                "postgres",
                "-c",
                "wal_level=logical",
                "-c",
                "max_replication_slots=10",
                "-c",
                "max_wal_senders=10",
            ])
            .with_copy_to(
                "/docker-entrypoint-initdb.d/replication.sh",
                PRIMARY_INIT_SCRIPT.as_bytes().to_vec(),
            )
            .with_network(&network)
            .with_container_name(&primary_host)
            .start()
            .await
            .expect("Failed to start primary container");
        let primary_port = primary
            .get_host_port_ipv4(5432)
            .await
            .expect("Failed to get primary port");

        let primary_config = pg_config(primary_port);
        create_database(&primary_config).await;
        migrate_etl(&primary_config)
            .await
            .expect("Failed to run ETL migrations");
        migrate_pgstream(&primary_config)
            .await
            .expect("Failed to run pgstream migrations");
        let pool = connect(&primary_config).await;
        create_today_partition(&pool)
            .await
            .expect("Failed to create pgstream partition for today");

        let standby = GenericImage::new("postgres", &format!("{version}-alpine"))
            .with_entrypoint("sh")
            .with_exposed_port(5432.tcp())
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept read-only connections",
            ))
            .with_cmd(["-c", &standby_script(&primary_host, sync_slots)])
            .with_network(&network)
            .with_container_name(format!("pgstream-standby-{suffix}"))
            .with_startup_timeout(Duration::from_secs(120))
            .start()
            .await
            .expect("Failed to start standby container");
        let standby_port = standby
            .get_host_port_ipv4(5432)
            .await
            .expect("Failed to get standby port");

        if sync_slots {
            // The slot must exist before it can be listed, so this is set after the base backup
            // created it.
            pool.execute(AssertSqlSafe(format!(
                "alter system set synchronized_standby_slots = '{STANDBY_SLOT_NAME}'"
            )))
            .await
            .expect("Failed to set synchronized_standby_slots");
            pool.execute("select pg_reload_conf()")
                .await
                .expect("Failed to reload configuration");
        }

        let pair = Self {
            primary,
            _standby: standby,
            primary_port,
            standby_port,
        };
        pair.wait_for_standby_replay().await;
        pair
    }

    /// Connection config for the test database on the primary.
    #[must_use]
    pub fn primary_config(&self) -> PgConnectionConfig {
        pg_config(self.primary_port)
    }

    /// Connection config for the test database on the standby.
    #[must_use]
    pub fn standby_config(&self) -> PgConnectionConfig {
        pg_config(self.standby_port)
    }

    /// Connects to the test database on the primary.
    pub async fn primary_pool(&self) -> PgPool {
        connect(&self.primary_config()).await
    }

    /// Connects to the test database on the standby.
    pub async fn standby_pool(&self) -> PgPool {
        connect(&self.standby_config()).await
    }

    /// Waits until the standby replayed all WAL the primary has written so far.
    pub async fn wait_for_standby_replay(&self) {
        let primary = self.primary_pool().await;
        let standby = self.standby_pool().await;
        let target: String = sqlx::query_scalar("select pg_current_wal_lsn()::text")
            .fetch_one(&primary)
            .await
            .expect("Failed to read primary WAL position");

        let result = tokio::time::timeout(SYNC_TIMEOUT, async {
            loop {
                let replayed: bool = sqlx::query_scalar(
                    "select coalesce(pg_last_wal_replay_lsn() >= $1::pg_lsn, false)",
                )
                .bind(&target)
                .fetch_one(&standby)
                .await
                .expect("Failed to read standby replay position");
                if replayed {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;

        assert!(
            result.is_ok(),
            "standby should replay the primary's WAL up to {target}"
        );
    }

    /// Waits until the standby holds a persistent, synced copy of `slot_name` that caught up
    /// with the slot's `confirmed_flush_lsn` on the primary.
    pub async fn wait_for_slot_synced(&self, slot_name: &str) {
        let primary = self.primary_pool().await;
        let standby = self.standby_pool().await;
        let target: String = sqlx::query_scalar(
            "select confirmed_flush_lsn::text from pg_replication_slots where slot_name = $1",
        )
        .bind(slot_name)
        .fetch_one(&primary)
        .await
        .expect("Slot should exist on the primary");

        let result = tokio::time::timeout(SYNC_TIMEOUT, async {
            loop {
                if slot_synced(&standby, slot_name, &target).await {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await;

        assert!(
            result.is_ok(),
            "slot {slot_name} should be synced to the standby up to {target}"
        );
    }

    /// Switches over to the standby: waits until it replayed all WAL, stops the primary and
    /// promotes the standby.
    pub async fn promote_standby(&self) {
        self.wait_for_standby_replay().await;

        // The postgres image stops with SIGINT, a fast shutdown that sends all remaining WAL to
        // the standby before exiting.
        self.primary
            .stop()
            .await
            .expect("Failed to stop primary container");

        let standby = self.standby_pool().await;
        let promoted: bool = sqlx::query_scalar("select pg_promote(true, 60)")
            .fetch_one(&standby)
            .await
            .expect("Failed to promote standby");
        assert!(promoted, "standby should be promoted");
    }
}

/// Shell script run by the standby container: takes a base backup of the primary through a new
/// physical slot, then starts Postgres in standby mode.
fn standby_script(primary_host: &str, sync_slots: bool) -> String {
    let conninfo = format!(
        "host={primary_host} port=5432 user=postgres password=postgres dbname=postgres \
         application_name=pgstream_test_standby"
    );
    let sync = if sync_slots { "on" } else { "off" };

    format!(
        r#"set -e
mkdir -p "$PGDATA"
chown postgres:postgres "$PGDATA"
chmod 700 "$PGDATA"
gosu postgres pg_basebackup --pgdata="$PGDATA" --checkpoint=fast --wal-method=stream --write-recovery-conf \
  --create-slot --slot={STANDBY_SLOT_NAME} --dbname="{conninfo}"
exec gosu postgres postgres \
  -c wal_level=logical \
  -c max_replication_slots=10 \
  -c max_wal_senders=10 \
  -c hot_standby_feedback=on \
  -c sync_replication_slots={sync} \
  -c primary_slot_name={STANDBY_SLOT_NAME} \
  -c "primary_conninfo={conninfo}"
"#
    )
}

/// Returns whether the standby's copy of `slot_name` is synced, persistent and at `target`.
async fn slot_synced(standby: &PgPool, slot_name: &str, target: &str) -> bool {
    sqlx::query_scalar(
        "select synced and not temporary and confirmed_flush_lsn >= $2::pg_lsn \
         from pg_replication_slots where slot_name = $1",
    )
    .bind(slot_name)
    .bind(target)
    .fetch_optional(standby)
    .await
    .expect("Failed to read standby slot")
    .unwrap_or(false)
}

fn pg_config(port: u16) -> PgConnectionConfig {
    PgConnectionConfig {
        host: "127.0.0.1".to_owned(),
        hostaddr: None,
        port,
        name: DATABASE_NAME.to_owned(),
        username: "postgres".to_owned(),
        password: Some("postgres".into()),
        tls: TlsConfig {
            trusted_root_certs: "".into(),
            enabled: false,
        },
        keepalive: TcpKeepaliveConfig::default(),
    }
}

async fn create_database(config: &PgConnectionConfig) {
    let mut connection = PgConnection::connect_with(&config.without_db(None))
        .await
        .expect("Failed to connect to Postgres (admin)");
    connection
        .execute(AssertSqlSafe(format!(
            r#"create database "{}";"#,
            config.name
        )))
        .await
        .expect("Failed to create test database");
}

async fn connect(config: &PgConnectionConfig) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect_with(config.with_db(None))
        .await
        .expect("Failed to connect to test database")
}
