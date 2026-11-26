mod auth;
mod model;
mod service;
mod session;
#[cfg(test)]
mod tests;

pub mod pb {
    tonic::include_proto!("prime.asr.v1");
}

use crate::auth::{TokenInterceptor, generate_token};
use crate::model::load_models;
use crate::pb::asr_service_server::AsrServiceServer;
use crate::service::AsrGrpcService;
use serde::Serialize;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

struct CliArgs {
    asr_path: PathBuf,
    vad_path: PathBuf,
    threads: usize,
    watch_stdin: bool,
}

#[derive(Serialize)]
struct Handshake {
    addr: String,
    token: String,
    langs: Vec<String>,
    chunk_ms: u32,
}

fn default_threads() -> usize {
    thread::available_parallelism()
        .map(|count| count.get().clamp(1, 4))
        .unwrap_or(2)
}

fn parse_args() -> Result<CliArgs, String> {
    let mut asr_path: Option<PathBuf> = None;
    let mut vad_path: Option<PathBuf> = None;
    let mut threads = default_threads();
    let mut watch_stdin = true;
    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--asr" => {
                let value = args.next().ok_or("missing value for --asr")?;
                asr_path = Some(PathBuf::from(value));
            }
            "--vad" => {
                let value = args.next().ok_or("missing value for --vad")?;
                vad_path = Some(PathBuf::from(value));
            }
            "--threads" => {
                let value = args.next().ok_or("missing value for --threads")?;
                threads = value
                    .parse::<usize>()
                    .map_err(|_| format!("invalid --threads value: {value}"))?
                    .max(1);
            }
            "--no-stdin-watch" => {
                watch_stdin = false;
            }
            "--version" => {
                println!("prime-asr {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            other => {
                return Err(format!("unknown argument: {other}"));
            }
        }
    }

    Ok(CliArgs {
        asr_path: asr_path.ok_or("missing required --asr <path>")?,
        vad_path: vad_path.ok_or("missing required --vad <path>")?,
        threads,
        watch_stdin,
    })
}

fn spawn_stdin_watcher(shutdown: Arc<Notify>) {
    thread::spawn(move || {
        let mut stdin = io::stdin();
        let mut buf = [0u8; 64];
        loop {
            match stdin.read(&mut buf) {
                Ok(0) | Err(_) => {
                    shutdown.notify_waiters();
                    break;
                }
                Ok(_) => {}
            }
        }
    });
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("prime-asr error: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let args = parse_args()?;
    let models = load_models(&args.asr_path, &args.vad_path, args.threads)?;
    let token = generate_token()?;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|err| format!("failed to bind loopback listener: {err}"))?;
    let local_addr = listener
        .local_addr()
        .map_err(|err| format!("failed to read listener addr: {err}"))?;

    let handshake = Handshake {
        addr: local_addr.to_string(),
        token: token.clone(),
        langs: models.langs().into_iter().map(str::to_string).collect(),
        chunk_ms: models.chunk_ms(),
    };
    let line = serde_json::to_string(&handshake)
        .map_err(|err| format!("failed to serialize handshake: {err}"))?;
    {
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "{line}").map_err(|err| format!("failed to write handshake: {err}"))?;
        stdout
            .flush()
            .map_err(|err| format!("failed to flush handshake: {err}"))?;
    }

    let shutdown = Arc::new(Notify::new());
    if args.watch_stdin {
        spawn_stdin_watcher(Arc::clone(&shutdown));
    }

    let service = AsrGrpcService::new(models, Arc::clone(&shutdown));
    let interceptor = TokenInterceptor::new(token);
    let incoming = TcpListenerStream::new(listener);

    Server::builder()
        .add_service(AsrServiceServer::with_interceptor(service, interceptor))
        .serve_with_incoming_shutdown(incoming, async move {
            tokio::select! {
                _ = shutdown.notified() => {},
                _ = tokio::signal::ctrl_c() => {},
            }
        })
        .await
        .map_err(|err| format!("gRPC server error: {err}"))
}
