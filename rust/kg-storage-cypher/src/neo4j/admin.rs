//! Neo4j operational commands; not portable openCypher.

pub const SERVER_VERSION: &str =
    "CALL dbms.components() YIELD name, versions RETURN name, versions";
pub const AWAIT_INDEXES: &str = "CALL db.awaitIndexes(60)";
pub const FULLTEXT_INDEXES: &str = "SHOW FULLTEXT INDEXES YIELD name, state, entityType, labelsOrTypes, properties, options RETURN name, state, entityType, labelsOrTypes, properties, options";
pub const VECTOR_INDEXES: &str = "SHOW VECTOR INDEXES YIELD name,state,entityType,labelsOrTypes,properties,options RETURN name,state,entityType,labelsOrTypes,properties,options";
pub const HEALTH: &str = "RETURN 1";
pub const CURRENT_USER_ROLES: &str = "SHOW CURRENT USER YIELD roles RETURN roles";
const TERMINATE_MARKED: &str = "SHOW TRANSACTIONS YIELD transactionId AS id, currentQuery WHERE currentQuery STARTS WITH $marker TERMINATE TRANSACTIONS id YIELD message RETURN message";

/// Only known schema identifiers may enter DDL text.
pub fn drop_search_index(name: &str) -> Result<String, kg_core::errors::BackendError> {
    if !super::schema::SEARCH_INDEXES.contains(&name) {
        return Err(kg_core::errors::BackendError::Query(
            "unknown search index".into(),
        ));
    }
    Ok(format!("DROP INDEX {name} IF EXISTS"))
}

pub fn terminate_marked(marker: &str) -> crate::PreparedQuery {
    crate::PreparedQuery {
        statement: TERMINATE_MARKED.into(),
        parameters: serde_json::json!({"marker": marker}),
    }
}

pub const CONSTRAINTS: &str = "SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, properties RETURN name, type, entityType, labelsOrTypes, properties";

/// A missing transaction is already finished; any other reply requires investigation.
pub fn termination_confirmed(row: &serde_json::Map<String, serde_json::Value>) -> bool {
    matches!(
        row.get("message").and_then(|value| value.as_str()),
        Some("Transaction terminated." | "Transaction not found.")
    )
}

pub const RANGE_INDEXES: &str = "SHOW RANGE INDEXES YIELD name,type,state,entityType,labelsOrTypes,properties RETURN name,type,state,entityType,labelsOrTypes,properties";

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn startup_inspects_all_fulltext_indexes_before_validating_required_definitions() {
        assert!(FULLTEXT_INDEXES.starts_with("SHOW FULLTEXT INDEXES"));
        assert!(!FULLTEXT_INDEXES.contains("WHERE name"));
    }
}
