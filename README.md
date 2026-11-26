# prime-asr

Standalone local gRPC speech recognition server built on [`dkaraush/tinyasr`](https://github.com/dkaraush/tinyasr).

## Features

- On-device speech recognition with Zipformer2 int4 and Silero VAD (`en-ru-uk` supported out of the box)
- Bidirectional gRPC streaming (`StreamRecognize`) with live partial updates, committed sentences, language detection, and end-of-turn events
- One-shot offline transcription (`Transcribe`)
- Local loopback binding (`127.0.0.1:0`) with per-process random `x-prime-asr-token` authentication
- Automatic shutdown on parent stdin EOF or `Shutdown` RPC

## Usage

```bash
prime-asr --asr path/to/en-ru-uk.tzf.gz --vad path/to/silero_vad.onnx.gz --threads 4
```

On startup, `prime-asr` prints a single JSON handshake line to `stdout`:

```json
{"addr":"127.0.0.1:54321","token":"<64-hex>","langs":["en","ru","uk"],"chunk_ms":320}
```

Clients connect via gRPC (`proto/asr.proto`) and pass `x-prime-asr-token: <token>` in request metadata.

## License

Licensed under the [GNU General Public License v3.0 or later](LICENSE).
Speech recognition engine and models are from [`dkaraush/tinyasr`](https://github.com/dkaraush/tinyasr) (GPL-3.0-or-later).
