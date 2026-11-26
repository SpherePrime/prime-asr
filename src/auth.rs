use tonic::service::Interceptor;
use tonic::{Request, Status};

pub const TOKEN_HEADER: &str = "x-prime-asr-token";

pub fn generate_token() -> Result<String, String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|err| format!("failed to generate auth token: {err}"))?;
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    Ok(hex)
}

#[derive(Clone)]
pub struct TokenInterceptor {
    expected: Vec<u8>,
}

impl TokenInterceptor {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            expected: token.into().into_bytes(),
        }
    }

    fn verify(&self, candidate: &[u8]) -> bool {
        if candidate.len() != self.expected.len() {
            return false;
        }
        let mut diff = 0u8;
        for (&a, &b) in candidate.iter().zip(self.expected.iter()) {
            diff |= a ^ b;
        }
        diff == 0
    }
}

impl Interceptor for TokenInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let Some(value) = request.metadata().get(TOKEN_HEADER) else {
            return Err(Status::unauthenticated("missing auth token"));
        };
        if !self.verify(value.as_bytes()) {
            return Err(Status::unauthenticated("invalid auth token"));
        }
        Ok(request)
    }
}
