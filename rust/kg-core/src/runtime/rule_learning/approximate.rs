//! Bounded approximate candidate discovery for rule proposals.
//!
//! When exact key matching finds nothing, approximate retrieval can still
//! *propose* that two identifiers are related — usually by a fixed prefix/suffix
//! transform. These are retrieval scores, kept strictly separate from identity
//! proof: an approximate match may propose a mapping or a
//! [`ValueTransform`](super::transform::ValueTransform), but it never becomes an
//! exact identity match on its own, and it never claims uniqueness. Retrieval is
//! bounded and reports truncation so a partial scan cannot be read as complete.
use serde::{Deserialize, Serialize};

use crate::runtime::rule_learning::transform::ValueTransform;

/// One approximate match. `score` is a retrieval score in `[0, 1]`, never a
/// probability of identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApproximateMatch {
    pub candidate: String,
    pub shared_prefix: usize,
    pub shared_suffix: usize,
    pub score: f64,
}

/// A bounded approximate-retrieval result. `truncated` is true when the corpus
/// was larger than the scan limit; a uniqueness claim is never valid from it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApproximateResult {
    pub matches: Vec<ApproximateMatch>,
    pub truncated: bool,
}

/// Approximate retrieval over candidate identifiers. Behind a trait so a real
/// backend (BM25, prefix index) can supply it; scores stay separate from
/// identity and callers must validate any proposed transform before trusting it.
pub trait ApproximateCandidateSource {
    fn search(&self, query: &str, limit: usize) -> ApproximateResult;
}

fn shared_prefix_len(a: &str, b: &str) -> usize {
    a.bytes().zip(b.bytes()).take_while(|(x, y)| x == y).count()
}

fn shared_suffix_len(a: &str, b: &str) -> usize {
    a.bytes()
        .rev()
        .zip(b.bytes().rev())
        .take_while(|(x, y)| x == y)
        .count()
}

/// Score a query against one candidate by shared prefix/suffix overlap,
/// normalized by the longer string. Exact equality scores 1.0 but is reported as
/// a match, never promoted to identity by this module.
pub fn overlap_score(query: &str, candidate: &str) -> ApproximateMatch {
    let prefix = shared_prefix_len(query, candidate);
    let suffix = shared_suffix_len(query, candidate);
    let longest = query.len().max(candidate.len()).max(1);
    // Do not double-count overlap when the strings are (near) identical.
    let overlap = (prefix + suffix).min(query.len().min(candidate.len()));
    ApproximateMatch {
        candidate: candidate.to_string(),
        shared_prefix: prefix,
        shared_suffix: suffix,
        score: overlap as f64 / longest as f64,
    }
}

/// A pure prefix/suffix index over a bounded corpus, for offline proposal and
/// tests. Returns the top `limit` matches by score, marking truncation.
#[derive(Debug, Default)]
pub struct OverlapIndex {
    corpus: Vec<String>,
}

impl OverlapIndex {
    pub fn new(corpus: impl IntoIterator<Item = String>) -> Self {
        Self {
            corpus: corpus.into_iter().collect(),
        }
    }
}

impl ApproximateCandidateSource for OverlapIndex {
    fn search(&self, query: &str, limit: usize) -> ApproximateResult {
        let mut scored: Vec<ApproximateMatch> = self
            .corpus
            .iter()
            .map(|c| overlap_score(query, c))
            .filter(|m| m.score > 0.0)
            .collect();
        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.candidate.cmp(&b.candidate))
        });
        let truncated = scored.len() > limit;
        scored.truncate(limit);
        ApproximateResult {
            matches: scored,
            truncated,
        }
    }
}

/// Propose a safe transform relating `query` to `candidate`, or `None` when no
/// supported transform explains the relation. The proposal must still be
/// validated on examples before it is trusted; it never becomes identity here.
pub fn suggest_transform(query: &str, candidate: &str) -> Option<ValueTransform> {
    if query == candidate {
        return Some(ValueTransform::Identity);
    }
    if query.eq_ignore_ascii_case(candidate) {
        return Some(ValueTransform::Lowercase);
    }
    if let Some(prefix) = query.strip_suffix(candidate) {
        // query = prefix + candidate  ->  strip the prefix to reach candidate.
        if !prefix.is_empty() {
            return ValueTransform::parse_supported("strip_prefix", Some(prefix));
        }
    }
    if let Some(suffix) = query.strip_prefix(candidate) {
        if !suffix.is_empty() {
            return ValueTransform::parse_supported("strip_suffix", Some(suffix));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retrieval_is_bounded_and_marks_truncation() {
        let index = OverlapIndex::new(
            ["arn:aws:i-1", "arn:aws:i-2", "arn:aws:i-3", "zzz"].map(String::from),
        );
        let result = index.search("arn:aws:i-1", 2);
        assert_eq!(result.matches.len(), 2);
        assert!(
            result.truncated,
            "corpus larger than the limit is truncated"
        );
        // The exact string scores highest but is only a retrieval match.
        assert_eq!(result.matches[0].candidate, "arn:aws:i-1");
        assert!(result.matches[0].score >= result.matches[1].score);
    }

    #[test]
    fn transforms_are_only_the_safe_supported_kinds() {
        assert_eq!(
            suggest_transform("arn:aws:i-1", "i-1"),
            Some(ValueTransform::StripPrefix {
                prefix: "arn:aws:".into()
            })
        );
        assert_eq!(
            suggest_transform("host.example", "host"),
            Some(ValueTransform::StripSuffix {
                suffix: ".example".into()
            })
        );
        assert_eq!(
            suggest_transform("ATLAS", "atlas"),
            Some(ValueTransform::Lowercase)
        );
        assert_eq!(suggest_transform("totally", "different"), None);
    }

    #[test]
    fn score_is_a_retrieval_score_not_identity() {
        let m = overlap_score("arn:aws:i-1", "arn:aws:i-9");
        assert!(m.score > 0.0 && m.score < 1.0);
        assert!(m.shared_prefix >= "arn:aws:i-".len());
    }
}
