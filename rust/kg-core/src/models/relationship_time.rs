//! Effective-time evidence is separate from observation clocks and relationship identity.
use chrono::{DateTime, Datelike, Timelike, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimePrecision {
    Instant,
    /// Normalized to midnight UTC; does not claim a known time of day.
    Date,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeBasis {
    Explicit,
    Absolute,
    Relative,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationshipTimeBound {
    pub at: DateTime<Utc>,
    pub precision: TimePrecision,
    pub basis: TimeBasis,
    pub quote: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipTimeOutcome {
    Explicit,
    Inferred,
    /// Observed as ongoing; capture time is a fallback, not a claimed factual start.
    Ongoing,
    Unknown,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RelationshipTarget {
    StoredVersion { uuid: Uuid },
    PriorObservation { observation_uuid: Uuid },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationshipTimeEvidence {
    /// Exact stored version or prior observation selected only by relationship resolution.
    #[serde(default)]
    pub resolved_target: Option<RelationshipTarget>,
    pub snapshot_id: Uuid,
    pub captured_at: DateTime<Utc>,
    pub outcome: RelationshipTimeOutcome,
    pub start: Option<RelationshipTimeBound>,
    pub end: Option<RelationshipTimeBound>,
}

impl RelationshipTimeEvidence {
    /// Exact dates supplied through the typed relationship input contract.
    pub fn explicit(
        snapshot_id: Uuid,
        captured_at: DateTime<Utc>,
        start: DateTime<Utc>,
        end: Option<DateTime<Utc>>,
    ) -> Self {
        let bound = |at| RelationshipTimeBound {
            at,
            precision: TimePrecision::Instant,
            basis: TimeBasis::Explicit,
            quote: None,
        };
        Self {
            resolved_target: None,
            snapshot_id,
            captured_at,
            outcome: RelationshipTimeOutcome::Explicit,
            start: Some(bound(start)),
            end: end.map(bound),
        }
    }

    pub fn end_only(&self) -> bool {
        self.start.is_none() && self.end.is_some()
    }

    pub fn validate(&self) -> Result<(), String> {
        let quote_valid = |quote: &Option<String>| {
            quote
                .as_ref()
                .is_none_or(|q| !q.trim().is_empty() && q.len() <= 8192)
        };
        if self.snapshot_id.is_nil()
            || self.resolved_target.as_ref().is_some_and(|target| {
                !self.end_only()
                    || match target {
                        RelationshipTarget::StoredVersion { uuid } => uuid.is_nil(),
                        RelationshipTarget::PriorObservation { observation_uuid } => {
                            observation_uuid.is_nil()
                        }
                    }
            })
        {
            return Err("invalid relationship time provenance".into());
        }
        for bound in self.start.iter().chain(self.end.iter()) {
            if !(1..=9999).contains(&bound.at.year())
                || bound.at.nanosecond() >= 1_000_000_000
                || !quote_valid(&bound.quote)
                || (bound.basis != TimeBasis::Explicit && bound.quote.is_none())
                || (bound.precision == TimePrecision::Date
                    && (bound.at.hour() != 0
                        || bound.at.minute() != 0
                        || bound.at.second() != 0
                        || bound.at.nanosecond() != 0))
            {
                return Err("invalid relationship time bound".into());
            }
        }
        if self
            .start
            .as_ref()
            .zip(self.end.as_ref())
            .is_some_and(|(s, e)| e.at < s.at)
        {
            return Err("relationship time ends before it starts".into());
        }
        let valid = match self.outcome {
            RelationshipTimeOutcome::Explicit => {
                self.start.is_some()
                    && self
                        .start
                        .iter()
                        .chain(self.end.iter())
                        .all(|b| b.basis == TimeBasis::Explicit)
            }
            RelationshipTimeOutcome::Inferred => {
                (self.start.is_some() || self.end.is_some())
                    && self
                        .start
                        .iter()
                        .chain(self.end.iter())
                        .all(|b| b.basis != TimeBasis::Explicit)
            }
            RelationshipTimeOutcome::Ongoing
            | RelationshipTimeOutcome::Unknown
            | RelationshipTimeOutcome::Disabled => self.start.is_none() && self.end.is_none(),
        };
        if !valid {
            return Err("relationship time outcome disagrees with its bounds".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn evidence() -> RelationshipTimeEvidence {
        RelationshipTimeEvidence {
            resolved_target: None,
            snapshot_id: Uuid::new_v4(),
            captured_at: "2026-09-19T12:00:00Z".parse().unwrap(),
            outcome: RelationshipTimeOutcome::Inferred,
            start: None,
            end: Some(RelationshipTimeBound {
                at: "2026-09-18T00:00:00Z".parse().unwrap(),
                precision: TimePrecision::Date,
                basis: TimeBasis::Relative,
                quote: Some("stopped yesterday".into()),
            }),
        }
    }
    #[test]
    fn end_only_evidence_retains_unknown_start_and_roundtrips() {
        let mut evidence = evidence();
        evidence.resolved_target = Some(RelationshipTarget::StoredVersion {
            uuid: Uuid::new_v4(),
        });
        evidence.validate().unwrap();
        assert!(evidence.end_only());
        let json = serde_json::to_value(&evidence).unwrap();
        assert_eq!(
            serde_json::from_value::<RelationshipTimeEvidence>(json).unwrap(),
            evidence
        );
    }
    #[test]
    fn invalid_precision_provenance_and_outcomes_fail_validation() {
        let mut cases = Vec::new();
        let mut e = evidence();
        e.snapshot_id = Uuid::nil();
        cases.push(e);
        let mut e = evidence();
        e.end.as_mut().unwrap().quote = None;
        cases.push(e);
        let mut e = evidence();
        e.end.as_mut().unwrap().at = e.captured_at;
        cases.push(e);
        let mut e = evidence();
        e.outcome = RelationshipTimeOutcome::Unknown;
        cases.push(e);
        let mut e = evidence();
        e.outcome = RelationshipTimeOutcome::Explicit;
        cases.push(e);
        let mut e = evidence();
        e.start = Some(RelationshipTimeBound {
            at: e.captured_at,
            precision: TimePrecision::Instant,
            basis: TimeBasis::Relative,
            quote: Some("today".into()),
        });
        cases.push(e);
        let mut e = evidence();
        e.resolved_target = Some(RelationshipTarget::PriorObservation {
            observation_uuid: Uuid::nil(),
        });
        cases.push(e);
        for e in cases {
            assert!(e.validate().is_err());
        }
    }
}
