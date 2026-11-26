use flate2::read::GzDecoder;
use std::fs;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use tinyasr::{Files, Models, Word};

pub fn read_maybe_gzip(path: &Path) -> Result<Vec<u8>, String> {
    let raw = fs::read(path).map_err(|err| format!("failed to read {}: {err}", path.display()))?;
    if raw.len() >= 2 && raw[0] == 0x1f && raw[1] == 0x8b {
        let mut decoder = GzDecoder::new(&raw[..]);
        let mut out = Vec::new();
        decoder
            .read_to_end(&mut out)
            .map_err(|err| format!("failed to decompress {}: {err}", path.display()))?;
        return Ok(out);
    }
    Ok(raw)
}

pub fn load_models(asr_path: &Path, vad_path: &Path, threads: usize) -> Result<Arc<Models>, String> {
    let asr_bytes = read_maybe_gzip(asr_path)?;
    let vad_bytes = read_maybe_gzip(vad_path)?;
    let mut models = Models::load(Files {
        asr: &asr_bytes,
        vad: &vad_bytes,
    })
    .map_err(|err| format!("failed to load tinyasr models: {err}"))?;
    models.set_threads(threads.max(1));
    Ok(Arc::new(models))
}

pub fn normalize_language_code(raw: &str) -> String {
    let trimmed = raw.trim().to_ascii_lowercase();
    if trimmed.is_empty() || trimmed == "auto" {
        return String::new();
    }
    let primary = trimmed
        .split(['-', '_', ','])
        .next()
        .unwrap_or("")
        .trim();
    match primary {
        "ru" | "rus" | "russian" => "ru".to_string(),
        "en" | "eng" | "english" => "en".to_string(),
        "uk" | "ukr" | "ua" | "ukrainian" => "uk".to_string(),
        other => other.to_string(),
    }
}

pub fn resolve_languages(requested: &str, available: &[String]) -> Vec<String> {
    let normalized = normalize_language_code(requested);
    if normalized.is_empty() {
        return available.to_vec();
    }
    if let Some(matched) = available
        .iter()
        .find(|code| code.eq_ignore_ascii_case(&normalized))
    {
        return vec![matched.clone()];
    }
    available.to_vec()
}

pub fn pcm_s16le_to_f32(bytes: &[u8]) -> Vec<f32> {
    let sample_count = bytes.len() / 2;
    let mut out = Vec::with_capacity(sample_count);
    for chunk in bytes.chunks_exact(2) {
        let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
        out.push(f32::from(sample) / 32768.0);
    }
    out
}

pub fn join_words(words: &[Word]) -> String {
    let mut text = String::new();
    for word in words {
        let piece = word.text.trim();
        if piece.is_empty() {
            continue;
        }
        if !text.is_empty() && !word.join {
            text.push(' ');
        }
        text.push_str(piece);
    }
    text
}

pub fn append_segment(target: &mut String, segment: &str) {
    let trimmed = segment.trim();
    if trimmed.is_empty() {
        return;
    }
    if !target.is_empty() {
        target.push(' ');
    }
    target.push_str(trimmed);
}
