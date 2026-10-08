//! Indexed primary and alternative identities. Links retain historical versions;
//! liveness belongs to the version, so deletion never loses tombstone lookup.

pub(crate) const SYNC: &str = "WITH n,reduce(keys=[],h IN [n.identity_hash]+coalesce(n.identity_hashes,[]) | CASE WHEN h IS NULL OR h IN keys THEN keys ELSE keys+[h] END) AS hashes
    CALL {
        WITH n,hashes
        OPTIONAL MATCH (key:IdentityKey {org_id:n.org_id})-[link:IDENTIFIES]->(n)
        WHERE NOT key.hash IN hashes
        DELETE link
        RETURN count(link) AS removed
    }
    CALL {
        WITH n,hashes
        UNWIND hashes AS hash
        WITH n,hash ORDER BY hash
        MERGE (key:IdentityKey {org_id:n.org_id,hash:hash})
        SET key.hash=key.hash
        WITH key,n
        WHERE NOT (n.is_latest=true AND n.deleted_at IS NULL) OR NOT EXISTS {
            MATCH (key)-[:IDENTIFIES]->(other:Entity)
            WHERE other.org_id=n.org_id AND other.is_latest=true AND other.deleted_at IS NULL
                AND other.chain_id<>n.chain_id
        }
        MERGE (key)-[:IDENTIFIES]->(n)
        RETURN count(key) AS linked
    }
    CALL {
        WITH n
        OPTIONAL MATCH (value:IdentityValue {org_id:n.org_id})-[link:KEY_COMPONENT]->(n)
        WHERE NOT value.token IN coalesce(n.key_values,[])
        DELETE link
        RETURN count(link) AS removed_values
    }
    CALL {
        WITH n
        UNWIND coalesce(n.key_values,[]) AS token
        WITH DISTINCT n,token ORDER BY token
        MERGE (value:IdentityValue {org_id:n.org_id,token:token})
        SET value.folded_token=CASE WHEN token STARTS WITH 's:' THEN toLower(token) ELSE token END
        MERGE (value)-[:KEY_COMPONENT]->(n)
        RETURN count(value) AS indexed_values
    }
    RETURN linked=size(hashes) AS ok,linked<>size(hashes) AS identity_conflict";

pub(crate) fn sync(prefix: &str) -> String {
    format!("{prefix}\n{SYNC}")
}

pub const READY: &str = "MATCH (n:Entity) WHERE any(hash IN [n.identity_hash]+coalesce(n.identity_hashes,[]) WHERE hash IS NOT NULL AND NOT EXISTS { MATCH (:IdentityKey {org_id:n.org_id,hash:hash})-[:IDENTIFIES]->(n) }) RETURN n.uuid AS missing LIMIT 1";

/// Typed values existed before the indexed component links. A nonempty list
/// without all its links needs the bounded key-values backfill before lookup.
pub const VALUES_READY: &str = "MATCH (n:Entity) WHERE n.is_latest=true AND n.deleted_at IS NULL AND n.key_values IS NOT NULL AND any(token IN n.key_values WHERE NOT EXISTS { MATCH (:IdentityValue {org_id:n.org_id,token:token})-[:KEY_COMPONENT]->(n) }) RETURN n.uuid AS missing LIMIT 1";
