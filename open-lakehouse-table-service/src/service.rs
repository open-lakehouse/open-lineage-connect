use buffa::OwnedView;
use connectrpc::{ConnectError, Context};

use crate::lineage::v1::OpenLineageEventView;
use crate::table::v1::{
    TableWriterService, WriteBatchRequestView, WriteBatchResponse, WriteEventRequestView,
    WriteEventResponse,
};
use crate::writer::delta::DeltaWriter;
use crate::writer::schema::events_to_record_batch;

pub struct TableWriterServiceImpl {
    writer: DeltaWriter,
}

impl TableWriterServiceImpl {
    pub fn new(writer: DeltaWriter) -> Self {
        Self { writer }
    }
}

impl TableWriterService for TableWriterServiceImpl {
    async fn write_event(
        &self,
        ctx: Context,
        request: OwnedView<WriteEventRequestView<'static>>,
    ) -> Result<(WriteEventResponse, Context), ConnectError> {
        if !request.event.is_set() {
            return Err(ConnectError::invalid_argument("event is required"));
        }

        let event_ref: &OpenLineageEventView<'_> = &request.event;
        let batch = events_to_record_batch(std::slice::from_ref(event_ref))
            .map_err(|e| ConnectError::internal(format!("schema conversion: {e}")))?;

        self.writer.append(batch).await.map_err(|e| {
            tracing::error!("delta write failed: {e}");
            ConnectError::internal(format!("delta write: {e}"))
        })?;

        Ok((
            WriteEventResponse {
                status: "ok".into(),
                ..Default::default()
            },
            ctx,
        ))
    }

    async fn write_batch(
        &self,
        ctx: Context,
        request: OwnedView<WriteBatchRequestView<'static>>,
    ) -> Result<(WriteBatchResponse, Context), ConnectError> {
        if request.events.is_empty() {
            return Ok((
                WriteBatchResponse {
                    status: "ok".into(),
                    written: 0,
                    ..Default::default()
                },
                ctx,
            ));
        }

        let batch = events_to_record_batch(&request.events)
            .map_err(|e| ConnectError::internal(format!("schema conversion: {e}")))?;

        let count = batch.num_rows() as i32;

        self.writer.append(batch).await.map_err(|e| {
            tracing::error!("delta write failed: {e}");
            ConnectError::internal(format!("delta write: {e}"))
        })?;

        Ok((
            WriteBatchResponse {
                status: "ok".into(),
                written: count,
                ..Default::default()
            },
            ctx,
        ))
    }
}
