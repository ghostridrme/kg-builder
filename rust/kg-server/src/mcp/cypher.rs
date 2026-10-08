//! Restricted administrator query surface. This is not tenant isolation.
use super::*;

/// Require scalar columns, preventing unbounded whole-node/collect payloads before Bolt decoding.
/// Caller ORDER BY/LIMIT stays inside the subquery; the outer projection only bounds wire values.
pub(super) fn bounded_statement(query: &str, offset: usize) -> Result<String, CallToolResult> {
    validate_cypher(query)?;
    // Do not execute user-defined functions through WHERE/ORDER BY. Their external
    // side effects are not described by Neo4j's graph-write classification.
    for (index, _) in query.match_indices('(') {
        let prefix = query[..index].trim_end();
        let name = prefix
            .rsplit(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '.')
            .next()
            .unwrap_or("")
            .to_ascii_uppercase();
        if !name.is_empty()
            && !matches!(
                name.as_str(),
                "MATCH"
                    | "WHERE"
                    | "AND"
                    | "OR"
                    | "NOT"
                    | "IN"
                    | "COUNT"
                    | "TOLOWER"
                    | "TOUPPER"
                    | "SIZE"
                    | "LEFT"
                    | "RIGHT"
                    | "SUBSTRING"
                    | "TOSTRING"
                    | "COALESCE"
                    | "DATETIME"
                    | "DATE"
            )
        {
            return Err(tool_error("invalid_input","User-defined functions are unsupported; use built-in scalar predicates and bound parameters"));
        }
    }
    let upper = query.to_ascii_uppercase();
    if offset > 0 && !upper.contains(" ORDER BY ") {
        return Err(tool_error(
            "invalid_input",
            "Offset paging requires an explicit deterministic ORDER BY",
        ));
    }
    if query.contains('`') || query.contains('{') || query.contains('}') || query.contains('*') {
        return Err(tool_error("invalid_input","Use fixed-length MATCH with WHERE parameters and scalar RETURN columns; maps, whole records and wildcard paths are unsupported"));
    }
    let (_, tail) = upper
        .rsplit_once(" RETURN ")
        .ok_or_else(|| tool_error("invalid_input", "Expected RETURN columns"))?;
    let start = query.len() - tail.len();
    let tail = &query[start..];
    let stop = [" ORDER BY ", " SKIP ", " LIMIT "]
        .into_iter()
        .filter_map(|s| tail.to_ascii_uppercase().find(s))
        .min()
        .unwrap_or(tail.len());
    let ident = |s: &str| {
        !s.is_empty()
            && s.len() <= 80
            && s.chars()
                .enumerate()
                .all(|(i, c)| c.is_ascii_alphabetic() || c == '_' || (i > 0 && c.is_ascii_digit()))
    };
    let mut projections = Vec::new();
    for col in tail[..stop].split(',') {
        let col = col.trim();
        let upper = col.to_ascii_uppercase();
        let (expression, alias) = if let Some(i) = upper.find(" AS ") {
            (col[..i].trim(), col[i + 4..].trim())
        } else {
            (col, col)
        };
        let scalar = expression
            .split_once('.')
            .is_some_and(|(a, b)| ident(a) && ident(b));
        let count = expression.to_ascii_uppercase().starts_with("COUNT(")
            && expression.ends_with(')')
            && ident(&expression[6..expression.len() - 1]);
        if !scalar && !count || (alias != expression && !ident(alias)) {
            return Err(tool_error("invalid_input","RETURN supports variable.property or count(variable), optionally AS identifier; no full nodes, arrays, maps or functions"));
        }
        // Expression is either a validated scalar or validated count; the column identifier
        // is escaped by construction (backticks were rejected).
        let column = format!("`{alias}`");
        projections.push(format!("CASE WHEN {column} IS :: STRING THEN left({column},1000) WHEN {column} IS :: INTEGER OR {column} IS :: FLOAT OR {column} IS :: BOOLEAN THEN {column} ELSE null END AS {column}"));
    }
    if projections.is_empty() || projections.len() > 8 {
        return Err(tool_error("invalid_input", "Return 1..8 scalar columns"));
    }
    Ok(format!(
        "CALL {{ {query} }} RETURN {} SKIP $mcp_offset LIMIT 21",
        projections.join(",")
    ))
}
