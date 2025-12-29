use chrono::{DateTime, NaiveDate, Utc};
use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    store::schema::SchemaStore,
};
use tracing::info;

use crate::{
    config::{EVENTS_TABLE, SCHEMA_NAME},
    metrics,
    store::StreamStore,
};

const DAYS_AHEAD_TO_CREATE: u32 = 7;
const RETENTION_DAYS: u32 = 7;

/// Domain representation of a partition with its date-based metadata.
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub name: String,
    pub date: NaiveDate,
}

impl PartitionInfo {
    /// Parse partition name to extract date information.
    pub fn from_name(name: String) -> EtlResult<Self> {
        let date = Self::parse_date(&name)?;
        Ok(Self { name, date })
    }

    /// Create partition info for a specific date.
    #[must_use]
    pub fn for_date(base_table: &str, date: NaiveDate) -> Self {
        Self {
            name: Self::format_name(base_table, date),
            date,
        }
    }

    /// Get SQL range bounds for this daily partition.
    #[must_use]
    pub fn range_bounds(&self) -> (String, String) {
        let start = self.date.format("%Y-%m-%d").to_string();
        let end = (self.date + chrono::Duration::days(1))
            .format("%Y-%m-%d")
            .to_string();
        (start, end)
    }

    fn parse_date(partition_name: &str) -> EtlResult<NaiveDate> {
        let date_str = partition_name
            .rsplit('_')
            .next()
            .ok_or_else(|| etl_error!(ErrorKind::ConversionError, "Invalid partition name"))?;

        NaiveDate::parse_from_str(date_str, "%Y%m%d")
            .map_err(|e| etl_error!(ErrorKind::ConversionError, "Failed to parse date: {}", e))
    }

    fn format_name(base_table: &str, date: NaiveDate) -> String {
        format!("{}_{}", base_table, date.format("%Y%m%d"))
    }
}

/// Retention policy for managing partition lifecycle.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pub retention_days: u32,
}

impl RetentionPolicy {
    #[must_use]
    pub fn new(retention_days: u32) -> Self {
        Self { retention_days }
    }

    #[must_use]
    pub fn plan_maintenance(
        &self,
        partitions: Vec<PartitionInfo>,
        table_name: &str,
        days_ahead: u32,
        now: DateTime<Utc>,
    ) -> (Vec<PartitionInfo>, Vec<PartitionInfo>) {
        let cutoff = (now - chrono::Duration::days(self.retention_days as i64)).date_naive();
        let today = now.date_naive();

        let mut to_drop = Vec::new();
        let mut existing_dates = Vec::new();

        for partition in partitions {
            if partition.date < cutoff {
                to_drop.push(partition);
            } else {
                existing_dates.push(partition.date);
            }
        }

        let mut to_create = Vec::new();
        for days in 0..days_ahead {
            let target_date = today + chrono::Duration::days(days as i64);
            if !existing_dates.contains(&target_date) {
                to_create.push(PartitionInfo::for_date(table_name, target_date));
            }
        }

        (to_drop, to_create)
    }
}

/// Background maintenance task
///
/// - Drops partitions older than retention period
/// - Creates partitions for upcoming days
pub async fn run_maintenance<S: SchemaStore>(store: &StreamStore<S>) -> EtlResult<DateTime<Utc>> {
    let start = Utc::now();
    let stream_id = store.stream_id();

    let result = async {
        let existing_partitions = store.load_partitions(SCHEMA_NAME, EVENTS_TABLE).await?;

        let policy = RetentionPolicy::new(RETENTION_DAYS);
        let (to_drop, to_create) = policy.plan_maintenance(
            existing_partitions,
            EVENTS_TABLE,
            DAYS_AHEAD_TO_CREATE,
            start,
        );

        for partition in to_drop {
            info!("Dropping partition: {}", partition.name);
            store.delete_partition(SCHEMA_NAME, &partition.name).await?;
        }

        for partition in to_create {
            info!("Creating partition: {}", partition.name);
            store
                .create_partition(SCHEMA_NAME, EVENTS_TABLE, &partition)
                .await?;
        }

        EtlResult::Ok(start)
    }
    .await;

    let duration_milliseconds = (Utc::now() - start).num_milliseconds() as f64;

    match result {
        Ok(timestamp) => {
            metrics::record_maintenance_run(stream_id, duration_milliseconds, true);
            Ok(timestamp)
        }
        Err(e) => {
            metrics::record_maintenance_run(stream_id, duration_milliseconds, false);
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_partition_info_from_name_valid() {
        let result = PartitionInfo::from_name("events_20240315".to_string()).unwrap();
        assert_eq!(result.name, "events_20240315");
        assert_eq!(result.date, NaiveDate::from_ymd_opt(2024, 3, 15).unwrap());
    }

    #[test]
    fn test_partition_info_from_name_invalid() {
        let result = PartitionInfo::from_name("events_invalid".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_partition_info_from_name_missing_suffix() {
        let result = PartitionInfo::from_name("events".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_partition_info_for_date() {
        let date = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let partition = PartitionInfo::for_date("events", date);

        assert_eq!(partition.name, "events_20240315");
        assert_eq!(partition.date, date);
    }

    #[test]
    fn test_partition_info_range_bounds() {
        let date = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let partition = PartitionInfo::for_date("events", date);

        let (start, end) = partition.range_bounds();
        assert_eq!(start, "2024-03-15");
        assert_eq!(end, "2024-03-16");
    }

    #[test]
    fn test_retention_policy_plan_maintenance_drop_old() {
        let policy = RetentionPolicy::new(RETENTION_DAYS);
        let now = Utc.with_ymd_and_hms(2024, 3, 15, 12, 0, 0).unwrap();

        // Partitions: one old (should drop), one current, one future
        let partitions = vec![
            PartitionInfo::for_date("events", NaiveDate::from_ymd_opt(2024, 3, 1).unwrap()), // 14 days old
            PartitionInfo::for_date("events", NaiveDate::from_ymd_opt(2024, 3, 15).unwrap()), // today
            PartitionInfo::for_date("events", NaiveDate::from_ymd_opt(2024, 3, 16).unwrap()), // tomorrow
        ];

        let (to_drop, _to_create) =
            policy.plan_maintenance(partitions, "events", DAYS_AHEAD_TO_CREATE, now);

        assert_eq!(to_drop.len(), 1);
        assert_eq!(
            to_drop.first().expect("first element exists").date,
            NaiveDate::from_ymd_opt(2024, 3, 1).unwrap()
        );
    }

    #[test]
    fn test_retention_policy_plan_maintenance_create_future() {
        let policy = RetentionPolicy::new(RETENTION_DAYS);
        let now = Utc.with_ymd_and_hms(2024, 3, 15, 12, 0, 0).unwrap();

        // Only today exists, should create 6 more (days 1-6 ahead)
        let partitions = vec![PartitionInfo::for_date(
            "events",
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(),
        )];

        let (to_drop, to_create) =
            policy.plan_maintenance(partitions, "events", DAYS_AHEAD_TO_CREATE, now);

        assert_eq!(to_drop.len(), 0);
        assert_eq!(to_create.len(), 6);
        // Verify first and last created partitions
        assert_eq!(
            to_create.first().expect("first element exists").date,
            NaiveDate::from_ymd_opt(2024, 3, 16).unwrap()
        );
        assert_eq!(
            to_create.last().expect("last element exists").date,
            NaiveDate::from_ymd_opt(2024, 3, 21).unwrap()
        );
    }

    #[test]
    fn test_retention_policy_plan_maintenance_no_changes_needed() {
        let policy = RetentionPolicy::new(RETENTION_DAYS);
        let now = Utc.with_ymd_and_hms(2024, 3, 15, 12, 0, 0).unwrap();

        // All partitions already exist (today + 6 days ahead)
        let partitions = vec![
            PartitionInfo::for_date("events", NaiveDate::from_ymd_opt(2024, 3, 15).unwrap()),
            PartitionInfo::for_date("events", NaiveDate::from_ymd_opt(2024, 3, 16).unwrap()),
            PartitionInfo::for_date("events", NaiveDate::from_ymd_opt(2024, 3, 17).unwrap()),
            PartitionInfo::for_date("events", NaiveDate::from_ymd_opt(2024, 3, 18).unwrap()),
            PartitionInfo::for_date("events", NaiveDate::from_ymd_opt(2024, 3, 19).unwrap()),
            PartitionInfo::for_date("events", NaiveDate::from_ymd_opt(2024, 3, 20).unwrap()),
            PartitionInfo::for_date("events", NaiveDate::from_ymd_opt(2024, 3, 21).unwrap()),
        ];

        let (to_drop, to_create) =
            policy.plan_maintenance(partitions, "events", DAYS_AHEAD_TO_CREATE, now);

        assert_eq!(to_drop.len(), 0);
        assert_eq!(to_create.len(), 0);
    }

    #[test]
    fn test_retention_policy_plan_maintenance_at_retention_boundary() {
        let policy = RetentionPolicy::new(RETENTION_DAYS);
        let now = Utc.with_ymd_and_hms(2024, 3, 15, 12, 0, 0).unwrap();

        // Partition exactly at cutoff (7 days old) should be kept
        let partitions = vec![
            PartitionInfo::for_date("events", NaiveDate::from_ymd_opt(2024, 3, 8).unwrap()), // exactly 7 days old
            PartitionInfo::for_date("events", NaiveDate::from_ymd_opt(2024, 3, 7).unwrap()), // 8 days old - should drop
        ];

        // Using days_ahead=1 to focus on testing the retention boundary behavior
        let (to_drop, _to_create) = policy.plan_maintenance(partitions, "events", 1, now);

        assert_eq!(to_drop.len(), 1);
        assert_eq!(
            to_drop.first().expect("first element exists").date,
            NaiveDate::from_ymd_opt(2024, 3, 7).unwrap()
        );
    }

    #[test]
    fn test_retention_policy_combined_drop_and_create() {
        let policy = RetentionPolicy::new(RETENTION_DAYS);
        let now = Utc.with_ymd_and_hms(2024, 3, 15, 12, 0, 0).unwrap();

        let partitions = vec![
            PartitionInfo::for_date("events", NaiveDate::from_ymd_opt(2024, 3, 1).unwrap()), // old - drop
            PartitionInfo::for_date("events", NaiveDate::from_ymd_opt(2024, 3, 15).unwrap()), // today - keep
        ];

        let (to_drop, to_create) =
            policy.plan_maintenance(partitions, "events", DAYS_AHEAD_TO_CREATE, now);

        assert_eq!(to_drop.len(), 1);
        assert_eq!(to_create.len(), 6); // days 1-6 ahead
    }
}
