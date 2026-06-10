use std::sync::Arc;

use buffa::OwnedView;
use connectrpc::{ConnectError, RequestContext, Response, ServiceResult};

use crate::lineage::v1::OpenLineageEventView;
use crate::table::v1::{
    TableWriterService, WriteBatchRequestView, WriteBatchResponse, WriteEventRequestView,
    WriteEventResponse,
};
use crate::writer::schema::events_to_record_batch;
use crate::writer::sink::TableSink;

/// Connect-RPC service that fans every incoming Arrow `RecordBatch` out to
/// one or more `TableSink`s. v1 is fail-fast: the first sink error aborts
/// the RPC, mirroring the legacy single-`DeltaWriter` behavior so existing
/// `delta`-only deployments see no change.
pub struct TableWriterServiceImpl {
    sinks: Vec<Arc<dyn TableSink>>,
}

impl TableWriterServiceImpl {
    pub fn new(sinks: Vec<Arc<dyn TableSink>>) -> Self {
        Self { sinks }
    }
}

// Each handler returns a concrete `ServiceResult<…Response>`, which refines
// the trait's `impl Encodable<…>` return to the specific response type. That
// refinement is intentional, so opt out of the `refining_impl_trait` lint.
#[allow(refining_impl_trait)]
impl TableWriterService for TableWriterServiceImpl {
    async fn write_event(
        &self,
        _ctx: RequestContext,
        request: OwnedView<WriteEventRequestView<'static>>,
    ) -> ServiceResult<WriteEventResponse> {
        if !request.event.is_set() {
            return Err(ConnectError::invalid_argument("event is required"));
        }

        let event_ref: &OpenLineageEventView<'_> = &request.event;
        let batch = events_to_record_batch(std::slice::from_ref(event_ref))
            .map_err(|e| ConnectError::internal(format!("schema conversion: {e}")))?;

        for sink in &self.sinks {
            sink.append(batch.clone()).await.map_err(|e| {
                tracing::error!("{} write failed: {e}", sink.name());
                ConnectError::internal(format!("{}: {e}", sink.name()))
            })?;
        }

        Ok(Response::new(WriteEventResponse {
            status: "ok".into(),
            ..Default::default()
        }))
    }

    async fn write_batch(
        &self,
        _ctx: RequestContext,
        request: OwnedView<WriteBatchRequestView<'static>>,
    ) -> ServiceResult<WriteBatchResponse> {
        if request.events.is_empty() {
            return Ok(Response::new(WriteBatchResponse {
                status: "ok".into(),
                written: 0,
                ..Default::default()
            }));
        }

        let batch = events_to_record_batch(&request.events)
            .map_err(|e| ConnectError::internal(format!("schema conversion: {e}")))?;

        let count = batch.num_rows() as i32;

        for sink in &self.sinks {
            sink.append(batch.clone()).await.map_err(|e| {
                tracing::error!("{} write failed: {e}", sink.name());
                ConnectError::internal(format!("{}: {e}", sink.name()))
            })?;
        }

        Ok(Response::new(WriteBatchResponse {
            status: "ok".into(),
            written: count,
            ..Default::default()
        }))
    }
}
