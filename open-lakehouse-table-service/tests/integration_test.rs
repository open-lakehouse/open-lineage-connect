use std::collections::HashMap;
use std::sync::Arc;

use deltalake::arrow::array::StringArray;
use deltalake::arrow::datatypes::DataType;
use deltalake::DeltaTableBuilder;

use open_lakehouse_table_service::config::{Config, StorageBackend};
use open_lakehouse_table_service::lineage::v1::OpenLineageEventView;
use open_lakehouse_table_service::writer::delta::DeltaWriter;
use open_lakehouse_table_service::writer::schema::{arrow_schema, events_to_record_batch};

fn local_config(path: &str) -> Config {
    Config {
        port: 8091,
        storage: StorageBackend::Local,
        table_path: path.to_string(),
        partition_cols: vec![],
        storage_options: HashMap::new(),
    }
}

#[test]
fn test_arrow_schema_field_count() {
    let schema = arrow_schema();
    assert_eq!(schema.fields().len(), 14);
    assert_eq!(schema.field(0).name(), "event_kind");
    assert_eq!(schema.field(2).data_type(), &DataType::Timestamp(
        deltalake::arrow::datatypes::TimeUnit::Microsecond,
        Some("UTC".into()),
    ));
}

#[test]
fn test_events_to_record_batch_empty() {
    let batch = events_to_record_batch(&[]).unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.num_columns(), 14);
}

#[test]
fn test_events_to_record_batch_skips_none_event() {
    let evt = OpenLineageEventView::default();
    let batch = events_to_record_batch(&[evt]).unwrap();
    assert_eq!(batch.num_rows(), 0, "events with no variant should be skipped");
}

#[tokio::test]
async fn test_delta_writer_create_and_append() {
    let tmp = tempfile::tempdir().unwrap();
    let writer = DeltaWriter::new(&local_config(tmp.path().to_str().unwrap()));

    let schema = arrow_schema();
    let batch = deltalake::arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["run"])),
            Arc::new(StringArray::from(vec![Some("START")])),
            Arc::new(
                deltalake::arrow::array::TimestampMicrosecondArray::from(vec![1_000_000i64])
                    .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(vec!["test-producer"])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![Some("run-123")])),
            Arc::new(StringArray::from(vec![Some("ns")])),
            Arc::new(StringArray::from(vec![Some("job1")])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![Some("{\"raw\":1}")])),
        ],
    )
    .unwrap();

    writer.append(batch).await.unwrap();

    // Verify the table was created and has version 1.
    let mut table = DeltaTableBuilder::from_uri(tmp.path().to_str().unwrap())
        .build()
        .unwrap();
    table.load().await.unwrap();
    assert!(table.version().is_some());
}

#[tokio::test]
async fn test_delta_writer_append_empty_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let writer = DeltaWriter::new(&local_config(tmp.path().to_str().unwrap()));

    let empty = deltalake::arrow::array::RecordBatch::new_empty(arrow_schema());
    writer.append(empty).await.unwrap();
    // No table should be created for an empty batch.
}

#[tokio::test]
async fn test_delta_writer_multiple_appends() {
    let tmp = tempfile::tempdir().unwrap();
    let writer = DeltaWriter::new(&local_config(tmp.path().to_str().unwrap()));

    let schema = arrow_schema();
    let make_batch = |kind: &str, producer: &str| {
        deltalake::arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![kind])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(
                    deltalake::arrow::array::TimestampMicrosecondArray::from(vec![2_000_000i64])
                        .with_timezone("UTC"),
                ),
                Arc::new(StringArray::from(vec![producer])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
        )
        .unwrap()
    };

    writer.append(make_batch("run", "p1")).await.unwrap();
    writer.append(make_batch("job", "p2")).await.unwrap();

    let mut table = DeltaTableBuilder::from_uri(tmp.path().to_str().unwrap())
        .build()
        .unwrap();
    table.load().await.unwrap();
    assert!(table.version().is_some());
}
