use crate::pb::asr_service_server::AsrService;
use crate::pb::{
    RecognizeEvent, RecognizeRequest, ShutdownRequest, ShutdownResponse, StatusRequest,
    StatusResponse, TranscribeRequest, TranscribeResponse, recognize_request,
};
use crate::session::{SessionCommand, StreamWorker, transcribe_offline};
use std::sync::Arc;
use tinyasr::{Models, SAMPLE_RATE};
use tokio::sync::{Notify, mpsc};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status, Streaming};

pub struct AsrGrpcService {
    models: Arc<Models>,
    shutdown: Arc<Notify>,
}

impl AsrGrpcService {
    pub fn new(models: Arc<Models>, shutdown: Arc<Notify>) -> Self {
        Self { models, shutdown }
    }
}

#[tonic::async_trait]
impl AsrService for AsrGrpcService {
    type StreamRecognizeStream = UnboundedReceiverStream<Result<RecognizeEvent, Status>>;

    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let languages = self
            .models
            .langs()
            .into_iter()
            .map(str::to_string)
            .collect();
        Ok(Response::new(StatusResponse {
            ready: true,
            languages,
            chunk_ms: self.models.chunk_ms(),
            sample_rate: SAMPLE_RATE,
        }))
    }

    async fn stream_recognize(
        &self,
        request: Request<Streaming<RecognizeRequest>>,
    ) -> Result<Response<Self::StreamRecognizeStream>, Status> {
        let mut inbound = request.into_inner();
        let models = Arc::clone(&self.models);
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            let mut worker: Option<StreamWorker> = None;
            while let Some(item) = inbound.next().await {
                let message = match item {
                    Ok(msg) => msg,
                    Err(err) => {
                        let _ = event_tx.send(Err(err));
                        break;
                    }
                };
                match message.payload {
                    Some(recognize_request::Payload::Start(start)) => {
                        if let Some(existing) = worker.take() {
                            let _ = existing.send(SessionCommand::Cancel);
                        }
                        worker = Some(StreamWorker::spawn(
                            Arc::clone(&models),
                            start.language,
                            start.start_language,
                            event_tx.clone(),
                        ));
                    }
                    Some(recognize_request::Payload::Audio(chunk)) => {
                        let active = worker.get_or_insert_with(|| {
                            StreamWorker::spawn(
                                Arc::clone(&models),
                                String::new(),
                                String::new(),
                                event_tx.clone(),
                            )
                        });
                        if active.send(SessionCommand::Audio(chunk.pcm_s16le)).is_err() {
                            break;
                        }
                    }
                    Some(recognize_request::Payload::Finish(_)) => {
                        if let Some(active) = worker.take() {
                            let _ = active.send(SessionCommand::Finish);
                        } else {
                            let _ = event_tx.send(Ok(RecognizeEvent {
                                event: Some(crate::pb::recognize_event::Event::FinalTranscript(
                                    crate::pb::FinalTranscript {
                                        text: String::new(),
                                    },
                                )),
                            }));
                        }
                        break;
                    }
                    Some(recognize_request::Payload::Cancel(_)) => {
                        if let Some(active) = worker.take() {
                            let _ = active.send(SessionCommand::Cancel);
                        }
                        break;
                    }
                    None => {}
                }
            }
            if let Some(active) = worker.take() {
                let _ = active.send(SessionCommand::Cancel);
            }
        });

        Ok(Response::new(UnboundedReceiverStream::new(event_rx)))
    }

    async fn transcribe(
        &self,
        request: Request<TranscribeRequest>,
    ) -> Result<Response<TranscribeResponse>, Status> {
        let payload = request.into_inner();
        let models = Arc::clone(&self.models);
        let (text, language) = tokio::task::spawn_blocking(move || {
            transcribe_offline(models, &payload.pcm_s16le, &payload.language)
        })
        .await
        .map_err(|err| Status::internal(format!("transcription worker failed: {err}")))?;
        Ok(Response::new(TranscribeResponse { text, language }))
    }

    async fn shutdown(
        &self,
        _request: Request<ShutdownRequest>,
    ) -> Result<Response<ShutdownResponse>, Status> {
        self.shutdown.notify_waiters();
        Ok(Response::new(ShutdownResponse {}))
    }
}
