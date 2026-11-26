use crate::auth::{TOKEN_HEADER, TokenInterceptor, generate_token};
use crate::model::{
    append_segment, join_words, load_models, normalize_language_code, pcm_s16le_to_f32,
    resolve_languages,
};
use crate::pb::asr_service_client::AsrServiceClient;
use crate::pb::asr_service_server::AsrServiceServer;
use crate::pb::{
    AudioChunk, FinishSession, RecognizeRequest, ShutdownRequest, StartSession, StatusRequest,
    TranscribeRequest, recognize_event, recognize_request,
};
use crate::service::AsrGrpcService;
use std::path::PathBuf;
use std::sync::Arc;
use tinyasr::Word;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tonic::{Code, Request};

#[test]
fn language_normalization_and_resolution() {
    let available = vec!["en".to_string(), "ru".to_string(), "uk".to_string()];
    assert_eq!(normalize_language_code(""), "");
    assert_eq!(normalize_language_code("auto"), "");
    assert_eq!(normalize_language_code("ru-RU"), "ru");
    assert_eq!(normalize_language_code("en_US"), "en");
    assert_eq!(normalize_language_code("uk-UA"), "uk");
    assert_eq!(normalize_language_code("ua"), "uk");

    assert_eq!(resolve_languages("auto", &available), available);
    assert_eq!(resolve_languages("", &available), available);
    assert_eq!(resolve_languages("ru-RU", &available), vec!["ru"]);
    assert_eq!(resolve_languages("en", &available), vec!["en"]);
    assert_eq!(resolve_languages("uk", &available), vec!["uk"]);
    assert_eq!(resolve_languages("fr", &available), available);
}

#[test]
fn pcm_conversion_and_word_joining() {
    let bytes = [0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0xFF];
    let samples = pcm_s16le_to_f32(&bytes);
    assert_eq!(samples.len(), 3);
    assert!((samples[0] - 0.0).abs() < 1e-6);
    assert!((samples[1] - 0.5).abs() < 1e-6);
    assert!((samples[2] + 1.0).abs() < 1e-6);

    let words = vec![
        Word {
            text: "Привет,".to_string(),
            start: 0.0,
            end: 0.3,
            join: false,
        },
        Word {
            text: "мир!".to_string(),
            start: 0.3,
            end: 0.6,
            join: false,
        },
    ];
    assert_eq!(join_words(&words), "Привет, мир!");

    let mut combined = String::new();
    append_segment(&mut combined, "Первая фраза.");
    append_segment(&mut combined, "Вторая фраза.");
    assert_eq!(combined, "Первая фраза. Вторая фраза.");
}

#[test]
fn generated_tokens_are_unique_hex() {
    let first = generate_token().expect("token 1");
    let second = generate_token().expect("token 2");
    assert_eq!(first.len(), 64);
    assert_eq!(second.len(), 64);
    assert_ne!(first, second);
    assert!(first.chars().all(|ch| ch.is_ascii_hexdigit()));
}

#[tokio::test]
async fn grpc_roundtrip_and_auth_enforcement() {
    let models_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("models");
    let asr_path = models_dir.join("en-ru-uk.tzf.gz");
    let vad_path = models_dir.join("silero_vad.onnx.gz");
    if !asr_path.exists() || !vad_path.exists() {
        return;
    }

    let models = load_models(&asr_path, &vad_path, 2).expect("load models");
    let token = generate_token().expect("token");
    let shutdown = Arc::new(Notify::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");

    let server_shutdown = Arc::clone(&shutdown);
    let server_token = token.clone();
    let server_handle = tokio::spawn(async move {
        let svc = AsrGrpcService::new(models, Arc::clone(&server_shutdown));
        let interceptor = TokenInterceptor::new(server_token);
        Server::builder()
            .add_service(AsrServiceServer::with_interceptor(svc, interceptor))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                server_shutdown.notified().await;
            })
            .await
            .expect("serve");
    });

    let endpoint = format!("http://{addr}");
    let mut client = AsrServiceClient::connect(endpoint).await.expect("connect");

    let unauth_err = client
        .status(Request::new(StatusRequest {}))
        .await
        .expect_err("missing token must fail");
    assert_eq!(unauth_err.code(), Code::Unauthenticated);

    let mut wrong_req = Request::new(StatusRequest {});
    wrong_req
        .metadata_mut()
        .insert(TOKEN_HEADER, MetadataValue::try_from("bad-token").unwrap());
    let wrong_err = client
        .status(wrong_req)
        .await
        .expect_err("wrong token must fail");
    assert_eq!(wrong_err.code(), Code::Unauthenticated);

    let token_val = MetadataValue::try_from(token.as_str()).unwrap();
    let mut status_req = Request::new(StatusRequest {});
    status_req
        .metadata_mut()
        .insert(TOKEN_HEADER, token_val.clone());
    let status = client
        .status(status_req)
        .await
        .expect("status ok")
        .into_inner();
    assert!(status.ready);
    assert_eq!(status.sample_rate, 16_000);
    assert!(status.languages.contains(&"ru".to_string()));
    assert!(status.languages.contains(&"en".to_string()));
    assert!(status.languages.contains(&"uk".to_string()));

    let silence = vec![0u8; 16_000 * 2];
    let requests = tokio_stream::iter(vec![
        RecognizeRequest {
            payload: Some(recognize_request::Payload::Start(StartSession {
                language: "auto".to_string(),
                start_language: "ru".to_string(),
            })),
        },
        RecognizeRequest {
            payload: Some(recognize_request::Payload::Audio(AudioChunk {
                pcm_s16le: silence.clone(),
            })),
        },
        RecognizeRequest {
            payload: Some(recognize_request::Payload::Finish(FinishSession {})),
        },
    ]);
    let mut stream_req = Request::new(requests);
    stream_req
        .metadata_mut()
        .insert(TOKEN_HEADER, token_val.clone());
    let mut events = client
        .stream_recognize(stream_req)
        .await
        .expect("stream start")
        .into_inner();
    let mut saw_final = false;
    while let Some(item) = events.next().await {
        let ev = item.expect("event ok");
        if matches!(ev.event, Some(recognize_event::Event::FinalTranscript(_))) {
            saw_final = true;
        }
    }
    assert!(saw_final);

    let mut transcribe_req = Request::new(TranscribeRequest {
        pcm_s16le: silence,
        language: "ru".to_string(),
        start_language: String::new(),
    });
    transcribe_req
        .metadata_mut()
        .insert(TOKEN_HEADER, token_val.clone());
    let transcribed = client
        .transcribe(transcribe_req)
        .await
        .expect("transcribe ok")
        .into_inner();
    assert_eq!(transcribed.language, "ru");

    let mut shutdown_req = Request::new(ShutdownRequest {});
    shutdown_req
        .metadata_mut()
        .insert(TOKEN_HEADER, token_val);
    client.shutdown(shutdown_req).await.expect("shutdown ok");
    server_handle.await.expect("server exited cleanly");
}
