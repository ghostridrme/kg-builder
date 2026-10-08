use kg_core::search::ExcerptSelection;

pub const SOURCE_SCAN_CHARS: usize = 65_536;
const EXCERPT_CHARS: usize = 4096;

/// A bounded, unchanged slice of source text, with Unicode scalar offsets.
pub struct SourceExcerpt {
    pub content: String,
    pub start: usize,
    pub end: usize,
    pub selection: ExcerptSelection,
}

/// Select the earliest literal term match. Lowercase expansion maps back to original
/// characters, so lowercasing cannot shift the returned source offsets.
pub fn source_excerpt(source: &str, query: Option<&str>) -> SourceExcerpt {
    let chars: Vec<_> = source.chars().take(SOURCE_SCAN_CHARS).collect();
    // Whole-string lowercasing preserves context-sensitive forms such as final sigma.
    // That context changes the letter, but not its encoded length in the offset map.
    let folded = chars.iter().collect::<String>().to_lowercase();
    let mut origins = Vec::new();
    for (index, ch) in chars.iter().enumerate() {
        for lower in ch.to_lowercase() {
            origins.extend(std::iter::repeat_n(index, lower.len_utf8()));
        }
    }
    let matched = query
        .into_iter()
        .flat_map(str::split_whitespace)
        .filter_map(|term| {
            let term = term.to_lowercase();
            let offset = folded.find(&term)?;
            let start = origins[offset];
            let end = origins[offset + term.len() - 1] + 1;
            (end - start <= EXCERPT_CHARS).then_some((start, end))
        })
        .min();
    let start = matched.map_or(0, |(start, end)| {
        start
            .saturating_sub(256)
            .max(end.saturating_sub(EXCERPT_CHARS))
    });
    let end = (start + EXCERPT_CHARS).min(chars.len());
    SourceExcerpt {
        content: chars[start..end].iter().collect(),
        start,
        end,
        selection: if matched.is_some() {
            ExcerptSelection::Matched
        } else {
            ExcerptSelection::Fallback
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn late_unicode_match_preserves_original_offsets_and_escaping() {
        let source = format!("{}İ 🌍 Retry \\\"quoted\\\"\nend", "前".repeat(5000));
        let excerpt = source_excerpt(&source, Some("retry"));
        assert_eq!(excerpt.selection, ExcerptSelection::Matched);
        assert!(excerpt.start > 4096);
        assert!(excerpt.content.contains("Retry"));
        assert_eq!(
            excerpt.content,
            source
                .chars()
                .skip(excerpt.start)
                .take(excerpt.end - excerpt.start)
                .collect::<String>()
        );
    }

    #[test]
    fn earliest_match_wins_regardless_of_query_order() {
        let excerpt = source_excerpt("İ FIRST then second", Some("second first"));
        assert_eq!(excerpt.selection, ExcerptSelection::Matched);
        assert_eq!(excerpt.content, "İ FIRST then second");
        assert_eq!(excerpt.end, 19);
    }

    #[test]
    fn contextual_lowercase_matches_without_changing_source_offsets() {
        let source = format!("{}ΟΣ", "🌍".repeat(5000));
        let excerpt = source_excerpt(&source, Some("ΟΣ"));
        assert_eq!(excerpt.selection, ExcerptSelection::Matched);
        assert_eq!(excerpt.start, 4744);
        assert_eq!(excerpt.end, 5002);
        assert!(excerpt.content.ends_with("ΟΣ"));
    }

    #[test]
    fn long_matching_terms_remain_whole_and_scan_boundary_is_exact() {
        let term = "a".repeat(EXCERPT_CHARS);
        let source = format!("{}{term}", "x".repeat(5000));
        let excerpt = source_excerpt(&source, Some(&term));
        assert_eq!(excerpt.content, term);
        assert_eq!(excerpt.start, 5000);
        assert_eq!(excerpt.end, 9096);
        let source = format!("{}İ", "x".repeat(SOURCE_SCAN_CHARS - 1));
        let excerpt = source_excerpt(&source, Some("İ"));
        assert_eq!(excerpt.selection, ExcerptSelection::Matched);
        assert_eq!(excerpt.end, SOURCE_SCAN_CHARS);
        assert!(excerpt.content.ends_with('İ'));
    }

    #[test]
    fn absent_analyzer_only_and_out_of_budget_matches_are_fallbacks() {
        for query in [None, Some("running"), Some("absent")] {
            assert_eq!(
                source_excerpt("run", query).selection,
                ExcerptSelection::Fallback
            );
        }
        let source = format!("{}needle", "x".repeat(SOURCE_SCAN_CHARS));
        let excerpt = source_excerpt(&source, Some("needle"));
        assert_eq!(excerpt.selection, ExcerptSelection::Fallback);
        assert_eq!(excerpt.end, EXCERPT_CHARS);
        assert_eq!(source_excerpt("", Some("anything")).end, 0);
    }
}
