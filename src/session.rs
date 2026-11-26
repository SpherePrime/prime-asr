use crate::model::{
    append_segment, join_words, normalize_language_code, pcm_s16le_to_f32, resolve_languages,
};
use crate::pb::{
    CommitTranscript, EndOfUtterance, FinalTranscript, LanguageDetected, PartialTranscript,
    RecognizeEvent, recognize_event,
};
use std::sync::Arc;
use std::sync::mpsc as std_mpsc;
use std::thread;
use tinyasr::{DEFAULT_BEAM, Event, Models, Options, Session, Transcription};
use tokio::sync::mpsc::UnboundedSender;
use tonic::Status;

pub enum SessionCommand {
    Audio(Vec<u8>),
    Finish,
    Cancel,
}

pub struct StreamWorker {
    tx: std_mpsc::Sender<SessionCommand>,
}

impl StreamWorker {
    pub fn spawn(
        models: Arc<Models>,
        language: String,
        start_language: String,
        event_tx: UnboundedSender<Result<RecognizeEvent, Status>>,
    ) -> Self {
        let (tx, rx) = std_mpsc::channel::<SessionCommand>();
        thread::spawn(move || {
            run_session_loop(models, language, start_language, rx, event_tx);
        });
        Self { tx }
    }

    pub fn send(&self, command: SessionCommand) -> Result<(), String> {
        self.tx
            .send(command)
            .map_err(|_| "asr session worker closed".to_string())
    }
}

fn run_session_loop(
    models: Arc<Models>,
    language: String,
    start_language: String,
    rx: std_mpsc::Receiver<SessionCommand>,
    event_tx: UnboundedSender<Result<RecognizeEvent, Status>>,
) {
    let available: Vec<String> = models.langs().into_iter().map(str::to_string).collect();
    let selected = resolve_languages(&language, &available);
    let lang_refs: Vec<&str> = selected.iter().map(String::as_str).collect();
    let options = Options {
        start_lang: normalize_language_code(&start_language),
        ..Options::default()
    };
    let mut session = Session::new(&models, &lang_refs, options);
    let mut committed = String::new();

    while let Ok(command) = rx.recv() {
        match command {
            SessionCommand::Audio(pcm) => {
                if pcm.is_empty() {
                    continue;
                }
                let samples = pcm_s16le_to_f32(&pcm);
                let events = session.push(&samples);
                if !forward_events(events, &mut committed, &event_tx) {
                    return;
                }
            }
            SessionCommand::Finish => {
                let events = session.finish();
                let _ = forward_events(events, &mut committed, &event_tx);
                let _ = event_tx.send(Ok(RecognizeEvent {
                    event: Some(recognize_event::Event::FinalTranscript(FinalTranscript {
                        text: committed.trim().to_string(),
                    })),
                }));
                return;
            }
            SessionCommand::Cancel => {
                return;
            }
        }
    }
}

fn forward_events(
    events: Vec<Event>,
    committed: &mut String,
    event_tx: &UnboundedSender<Result<RecognizeEvent, Status>>,
) -> bool {
    for event in events {
        let proto_event = match event {
            Event::Commit(words) => {
                let text = join_words(&words);
                if text.is_empty() {
                    continue;
                }
                append_segment(committed, &text);
                recognize_event::Event::Commit(CommitTranscript { text })
            }
            Event::Partial { open, tail } => {
                recognize_event::Event::Partial(PartialTranscript {
                    committed: committed.clone(),
                    open: join_words(&open),
                    tail: join_words(&tail),
                })
            }
            Event::Language(lang) => {
                recognize_event::Event::Language(LanguageDetected { language: lang })
            }
            Event::Eou { at, p } => recognize_event::Event::Eou(EndOfUtterance {
                at_seconds: at,
                probability: p,
            }),
        };
        if event_tx
            .send(Ok(RecognizeEvent {
                event: Some(proto_event),
            }))
            .is_err()
        {
            return false;
        }
    }
    true
}

pub fn transcribe_offline(
    models: Arc<Models>,
    pcm_bytes: &[u8],
    language: &str,
) -> (String, String) {
    let available: Vec<String> = models.langs().into_iter().map(str::to_string).collect();
    let selected = resolve_languages(language, &available);
    let lang_refs: Vec<&str> = selected.iter().map(String::as_str).collect();
    let samples = pcm_s16le_to_f32(pcm_bytes);
    let mut transcription = Transcription::new(&models, &samples, &lang_refs, DEFAULT_BEAM);
    let mut combined = String::new();
    for segment in &mut transcription {
        let text = join_words(&segment);
        append_segment(&mut combined, &text);
    }
    (combined, transcription.lang().to_string())
}
