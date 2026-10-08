//! The paid-provider gate. A paid test is ignored by default; once selected it
//! must either run against the real provider or fail with the variable that
//! is missing. There is no "print skipped and return" path.

use std::fmt;

/// Positive opt-in for provider spend. Set only by the `paid:*` Taskfile targets.
pub const PAID_FLAG: &str = "KG_PAID";
/// The provider credential a paid test needs.
pub const OPENAI_KEY: &str = "OPENAI_API_KEY";

/// A missing or malformed environment prerequisite.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvError {
    pub variable: &'static str,
    pub reason: &'static str,
}

impl fmt::Display for EnvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.variable, self.reason)
    }
}

impl std::error::Error for EnvError {}

/// Credentials for a paid run.
#[derive(Clone)]
pub struct PaidProvider {
    pub openai_key: String,
}

impl fmt::Debug for PaidProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PaidProvider")
            .field("openai_key", &"<redacted>")
            .finish()
    }
}

/// Require `KG_PAID=1`, load `.env`, and return the provider key.
/// An output path alone is never authorization to spend.
pub fn paid() -> Result<PaidProvider, EnvError> {
    match std::env::var(PAID_FLAG) {
        Ok(v) if v == "1" => {}
        Ok(_) => {
            return Err(EnvError {
                variable: PAID_FLAG,
                reason: "must be exactly 1 to authorize provider spend",
            })
        }
        Err(_) => {
            return Err(EnvError {
                variable: PAID_FLAG,
                reason: "paid tests run only from `task paid:*` (sets KG_PAID=1)",
            })
        }
    }
    let _ = dotenvy::dotenv();
    let openai_key = std::env::var(OPENAI_KEY)
        .ok()
        .filter(|k| !k.trim().is_empty())
        .ok_or(EnvError {
            variable: OPENAI_KEY,
            reason: "not set (checked the environment and .env)",
        })?;
    Ok(PaidProvider { openai_key })
}

/// A required string variable, reported by name when missing or empty.
pub fn required(variable: &'static str) -> Result<String, EnvError> {
    std::env::var(variable)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .ok_or(EnvError {
            variable,
            reason: "required for this test and not set",
        })
}
