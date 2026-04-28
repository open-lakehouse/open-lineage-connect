use std::sync::Arc;

use deltalake::arrow::array::{
    ArrayRef, RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::lineage::v1::{
    DatasetEventView, InputDatasetView, JobEventView, OpenLineageEventView, OutputDatasetView,
    RunEventView,
};

pub fn arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("event_kind", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, true),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("producer", DataType::Utf8, false),
        Field::new("schema_url", DataType::Utf8, true),
        Field::new("run_id", DataType::Utf8, true),
        Field::new("job_namespace", DataType::Utf8, true),
        Field::new("job_name", DataType::Utf8, true),
        Field::new("dataset_namespace", DataType::Utf8, true),
        Field::new("dataset_name", DataType::Utf8, true),
        Field::new("facets_json", DataType::Utf8, true),
        Field::new("inputs_json", DataType::Utf8, true),
        Field::new("outputs_json", DataType::Utf8, true),
        Field::new("raw_json", DataType::Utf8, true),
    ]))
}

pub fn events_to_record_batch(
    events: &[OpenLineageEventView<'_>],
) -> Result<RecordBatch, String> {
    let schema = arrow_schema();
    let cap = events.len();

    let mut col_event_kind = StringBuilder::with_capacity(cap, cap * 8);
    let mut col_event_type = StringBuilder::with_capacity(cap, cap * 16);
    let mut col_event_time =
        TimestampMicrosecondBuilder::with_capacity(cap).with_timezone("UTC");
    let mut col_producer = StringBuilder::with_capacity(cap, cap * 32);
    let mut col_schema_url = StringBuilder::with_capacity(cap, cap * 64);
    let mut col_run_id = StringBuilder::with_capacity(cap, cap * 36);
    let mut col_job_ns = StringBuilder::with_capacity(cap, cap * 32);
    let mut col_job_name = StringBuilder::with_capacity(cap, cap * 32);
    let mut col_ds_ns = StringBuilder::with_capacity(cap, cap * 32);
    let mut col_ds_name = StringBuilder::with_capacity(cap, cap * 32);
    let mut col_facets = StringBuilder::with_capacity(cap, cap * 128);
    let mut col_inputs = StringBuilder::with_capacity(cap, cap * 128);
    let mut col_outputs = StringBuilder::with_capacity(cap, cap * 128);
    let mut col_raw = StringBuilder::with_capacity(cap, cap * 256);

    for evt in events {
        match &evt.event {
            Some(ev) => {
                use crate::lineage::v1::open_lineage_event::EventView;
                match ev {
                    EventView::RunEvent(re) => append_run(
                        re,
                        &mut col_event_kind,
                        &mut col_event_type,
                        &mut col_event_time,
                        &mut col_producer,
                        &mut col_schema_url,
                        &mut col_run_id,
                        &mut col_job_ns,
                        &mut col_job_name,
                        &mut col_ds_ns,
                        &mut col_ds_name,
                        &mut col_facets,
                        &mut col_inputs,
                        &mut col_outputs,
                        &mut col_raw,
                    ),
                    EventView::JobEvent(je) => append_job(
                        je,
                        &mut col_event_kind,
                        &mut col_event_type,
                        &mut col_event_time,
                        &mut col_producer,
                        &mut col_schema_url,
                        &mut col_run_id,
                        &mut col_job_ns,
                        &mut col_job_name,
                        &mut col_ds_ns,
                        &mut col_ds_name,
                        &mut col_facets,
                        &mut col_inputs,
                        &mut col_outputs,
                        &mut col_raw,
                    ),
                    EventView::DatasetEvent(de) => append_dataset(
                        de,
                        &mut col_event_kind,
                        &mut col_event_type,
                        &mut col_event_time,
                        &mut col_producer,
                        &mut col_schema_url,
                        &mut col_run_id,
                        &mut col_job_ns,
                        &mut col_job_name,
                        &mut col_ds_ns,
                        &mut col_ds_name,
                        &mut col_facets,
                        &mut col_inputs,
                        &mut col_outputs,
                        &mut col_raw,
                    ),
                }
            }
            None => continue,
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(col_event_kind.finish()),
        Arc::new(col_event_type.finish()),
        Arc::new(col_event_time.finish()),
        Arc::new(col_producer.finish()),
        Arc::new(col_schema_url.finish()),
        Arc::new(col_run_id.finish()),
        Arc::new(col_job_ns.finish()),
        Arc::new(col_job_name.finish()),
        Arc::new(col_ds_ns.finish()),
        Arc::new(col_ds_name.finish()),
        Arc::new(col_facets.finish()),
        Arc::new(col_inputs.finish()),
        Arc::new(col_outputs.finish()),
        Arc::new(col_raw.finish()),
    ];

    RecordBatch::try_new(schema, columns).map_err(|e| e.to_string())
}

fn ts_to_micros(ts: &buffa_types::google::protobuf::TimestampView<'_>) -> i64 {
    ts.seconds * 1_000_000 + (ts.nanos as i64) / 1_000
}

fn non_empty(s: &str) -> Option<&str> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

fn input_datasets_to_json(datasets: &[InputDatasetView<'_>]) -> Option<String> {
    if datasets.is_empty() {
        return None;
    }
    let arr: Vec<serde_json::Value> = datasets
        .iter()
        .map(|d| {
            let mut m = serde_json::Map::new();
            m.insert("namespace".into(), d.namespace.into());
            m.insert("name".into(), d.name.into());
            m
        })
        .map(serde_json::Value::Object)
        .collect();
    Some(serde_json::to_string(&arr).unwrap_or_default())
}

fn output_datasets_to_json(datasets: &[OutputDatasetView<'_>]) -> Option<String> {
    if datasets.is_empty() {
        return None;
    }
    let arr: Vec<serde_json::Value> = datasets
        .iter()
        .map(|d| {
            let mut m = serde_json::Map::new();
            m.insert("namespace".into(), d.namespace.into());
            m.insert("name".into(), d.name.into());
            m
        })
        .map(serde_json::Value::Object)
        .collect();
    Some(serde_json::to_string(&arr).unwrap_or_default())
}

#[allow(clippy::too_many_arguments)]
fn append_run(
    re: &RunEventView<'_>,
    kind: &mut StringBuilder,
    etype: &mut StringBuilder,
    etime: &mut TimestampMicrosecondBuilder,
    producer: &mut StringBuilder,
    schema_url: &mut StringBuilder,
    run_id: &mut StringBuilder,
    job_ns: &mut StringBuilder,
    job_name: &mut StringBuilder,
    ds_ns: &mut StringBuilder,
    ds_name: &mut StringBuilder,
    facets: &mut StringBuilder,
    inputs: &mut StringBuilder,
    outputs: &mut StringBuilder,
    raw: &mut StringBuilder,
) {
    kind.append_value("run");
    etype.append_value(re.event_type);

    if re.event_time.is_set() {
        etime.append_value(ts_to_micros(&re.event_time));
    } else {
        etime.append_null();
    }

    producer.append_value(re.producer);
    schema_url.append_option(non_empty(re.schema_url));

    if re.run.is_set() {
        run_id.append_option(non_empty(re.run.run_id));
    } else {
        run_id.append_null();
    }

    if re.job.is_set() {
        job_ns.append_option(non_empty(re.job.namespace));
        job_name.append_option(non_empty(re.job.name));
    } else {
        job_ns.append_null();
        job_name.append_null();
    }

    ds_ns.append_null();
    ds_name.append_null();
    facets.append_null();
    inputs.append_option(input_datasets_to_json(&re.inputs));
    outputs.append_option(output_datasets_to_json(&re.outputs));
    raw.append_option(non_empty(re.raw_json));
}

#[allow(clippy::too_many_arguments)]
fn append_job(
    je: &JobEventView<'_>,
    kind: &mut StringBuilder,
    etype: &mut StringBuilder,
    etime: &mut TimestampMicrosecondBuilder,
    producer: &mut StringBuilder,
    schema_url: &mut StringBuilder,
    run_id: &mut StringBuilder,
    job_ns: &mut StringBuilder,
    job_name: &mut StringBuilder,
    ds_ns: &mut StringBuilder,
    ds_name: &mut StringBuilder,
    facets: &mut StringBuilder,
    inputs: &mut StringBuilder,
    outputs: &mut StringBuilder,
    raw: &mut StringBuilder,
) {
    kind.append_value("job");
    etype.append_null();

    if je.event_time.is_set() {
        etime.append_value(ts_to_micros(&je.event_time));
    } else {
        etime.append_null();
    }

    producer.append_value(je.producer);
    schema_url.append_option(non_empty(je.schema_url));
    run_id.append_null();

    if je.job.is_set() {
        job_ns.append_option(non_empty(je.job.namespace));
        job_name.append_option(non_empty(je.job.name));
    } else {
        job_ns.append_null();
        job_name.append_null();
    }

    ds_ns.append_null();
    ds_name.append_null();
    facets.append_null();
    inputs.append_option(input_datasets_to_json(&je.inputs));
    outputs.append_option(output_datasets_to_json(&je.outputs));
    raw.append_option(non_empty(je.raw_json));
}

#[allow(clippy::too_many_arguments)]
fn append_dataset(
    de: &DatasetEventView<'_>,
    kind: &mut StringBuilder,
    etype: &mut StringBuilder,
    etime: &mut TimestampMicrosecondBuilder,
    producer: &mut StringBuilder,
    schema_url: &mut StringBuilder,
    run_id: &mut StringBuilder,
    job_ns: &mut StringBuilder,
    job_name: &mut StringBuilder,
    ds_ns: &mut StringBuilder,
    ds_name: &mut StringBuilder,
    facets: &mut StringBuilder,
    inputs: &mut StringBuilder,
    outputs: &mut StringBuilder,
    raw: &mut StringBuilder,
) {
    kind.append_value("dataset");
    etype.append_null();

    if de.event_time.is_set() {
        etime.append_value(ts_to_micros(&de.event_time));
    } else {
        etime.append_null();
    }

    producer.append_value(de.producer);
    schema_url.append_option(non_empty(de.schema_url));
    run_id.append_null();
    job_ns.append_null();
    job_name.append_null();

    if de.dataset.is_set() {
        ds_ns.append_option(non_empty(de.dataset.namespace));
        ds_name.append_option(non_empty(de.dataset.name));
    } else {
        ds_ns.append_null();
        ds_name.append_null();
    }

    facets.append_null();
    inputs.append_null();
    outputs.append_null();
    raw.append_option(non_empty(de.raw_json));
}
