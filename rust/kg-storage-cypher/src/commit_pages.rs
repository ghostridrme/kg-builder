//! Durable bounded commit plans and the graph-write fence used between pages.
use crate::{PreparedQuery, PreparedWrite};
use kg_core::{
    errors::BackendError,
    traits::{BatchIdentity, MutationBatch},
};
use serde_json::{json, Value};
use uuid::Uuid;
pub const PART_BYTES: usize = 4 * 1024 * 1024;
pub use kg_core::traits::commit_pages::MAX_PLAN_BYTES;
fn write(statement: &str, parameters: Value) -> PreparedWrite {
    PreparedWrite {
        statement: statement.into(),
        parameters,
        expected_rows: 1,
    }
}
pub fn lock_revision(org: &str, batch_id: Uuid, all: bool) -> PreparedWrite {
    let stripes: Vec<u8> = if all {
        (0..64).collect()
    } else {
        vec![batch_id.as_bytes()[0] % 64]
    };
    PreparedWrite {statement:"UNWIND $stripes AS stripe WITH stripe ORDER BY stripe MERGE (r:GraphRevision {org_id:$org,stripe:stripe}) ON CREATE SET r.token='' SET r.lock_count=coalesce(r.lock_count,0)+1 RETURN true AS ok".into(),parameters:json!({"org":org,"stripes":stripes}),expected_rows:if all{64}else{1}}
}
pub fn advance_revision(org: &str, token: Uuid) -> PreparedWrite {
    write(
        "MATCH (r:GraphRevision {org_id:$org,stripe:$stripe}) SET r.token=$token RETURN true AS ok",
        json!({"org":org,"stripe":token.as_bytes()[0]%64,"token":token}),
    )
}
pub fn read(org: &str, id: BatchIdentity) -> PreparedQuery {
    PreparedQuery {statement:"MATCH (p:CommitPlan {org_id:$org,batch_id:$id,ready:true}) RETURN p.digest AS digest,p.parts AS parts,p.fingerprint AS fingerprint,p.next_page AS next_page".into(),parameters:json!({"org":org,"id":id.batch_id()})}
}
pub fn part(org: &str, id: Uuid, index: usize) -> PreparedQuery {
    PreparedQuery {statement:"MATCH (p:CommitPlanPart {org_id:$org,batch_id:$id,ordinal:$index}) RETURN p.body AS body".into(),parameters:json!({"org":org,"id":id,"index":index})}
}
pub fn begin(batch: &MutationBatch, digest: Uuid, parts: usize) -> PreparedWrite {
    write("MERGE (p:CommitPlan {batch_id:$id}) ON CREATE SET p.org_id=$org,p.fingerprint=$fingerprint,p.digest=$digest,p.parts=$parts,p.next_page=0,p.ready=false SET p.lock_count=coalesce(p.lock_count,0)+1 WITH p WHERE p.org_id=$org AND p.fingerprint=$fingerprint AND p.digest=$digest AND p.parts=$parts RETURN true AS ok",json!({"id":batch.batch_id(),"org":batch.org_id,"fingerprint":batch.fingerprint.0,"digest":digest,"parts":parts}))
}
pub fn save_part(batch: &MutationBatch, digest: Uuid, index: usize, body: &str) -> PreparedWrite {
    write("MATCH (p:CommitPlan {org_id:$org,batch_id:$id,digest:$digest}) MERGE (c:CommitPlanPart {batch_id:$id,ordinal:$index}) ON CREATE SET c.org_id=$org,c.body=$body WITH c WHERE c.org_id=$org AND c.body=$body RETURN true AS ok",json!({"id":batch.batch_id(),"org":batch.org_id,"digest":digest,"index":index,"body":body}))
}
pub fn ready(batch: &MutationBatch, digest: Uuid, parts: usize) -> PreparedWrite {
    write("MATCH (p:CommitPlan {org_id:$org,batch_id:$id,digest:$digest}) MATCH (c:CommitPlanPart {batch_id:$id,org_id:$org}) WITH p,count(c) AS n WHERE n=$parts SET p.ready=true RETURN true AS ok",json!({"id":batch.batch_id(),"org":batch.org_id,"digest":digest,"parts":parts}))
}
pub fn guard(batch: &MutationBatch, page: usize) -> PreparedWrite {
    write("MATCH (p:CommitPlan {org_id:$org,batch_id:$id,ready:true,fingerprint:$fingerprint}) SET p.lock_count=coalesce(p.lock_count,0)+1 WITH p MATCH (r:GraphRevision {org_id:$org}) WITH p,r ORDER BY r.stripe WITH p,collect(r.token) AS tokens WHERE p.next_page=$page AND ($page=0 OR p.tokens=tokens) RETURN true AS ok",json!({"org":batch.org_id,"id":batch.batch_id(),"fingerprint":batch.fingerprint.0,"page":page}))
}
/// The parent receipt now owns recovery; do not retain a second copy of all
/// payloads and vectors once every page has committed.
pub fn release_parts(batch: &MutationBatch) -> PreparedWrite {
    write("MATCH (c:CommitPlanPart {org_id:$org,batch_id:$id}) WITH collect(c) AS parts FOREACH (c IN parts | DELETE c) RETURN true AS ok",json!({"org":batch.org_id,"id":batch.batch_id()}))
}
pub fn page_id(batch: Uuid, page: usize) -> Uuid {
    Uuid::new_v5(&batch, format!("commit-page:{page}").as_bytes())
}
pub fn read_receipt(batch: &MutationBatch, page: usize) -> PreparedQuery {
    PreparedQuery {statement:"MATCH (r:CommitPageReceipt {org_id:$org,batch_id:$id,ordinal:$page}) RETURN r.org_id AS org_id,r.fingerprint AS fingerprint,$id AS batch_id,r.run_id AS run_id,r.kind AS kind,r.index AS index,r.committed_at AS committed_at,'{}' AS result".into(),parameters:json!({"org":batch.org_id,"id":batch.batch_id(),"page":page})}
}
pub fn receipt(
    batch: &MutationBatch,
    page: usize,
    at: chrono::DateTime<chrono::Utc>,
) -> PreparedWrite {
    write("MATCH (p:CommitPlan {org_id:$org,batch_id:$id,next_page:$page}) CREATE (c:CommitPageReceipt {org_id:$org,batch_id:$id,ordinal:$page,fingerprint:$fingerprint,run_id:$run,kind:$kind,index:$index,committed_at:$at}) WITH p MATCH (r:GraphRevision {org_id:$org,stripe:$stripe}) SET r.token=$token WITH p MATCH (r:GraphRevision {org_id:$org}) WITH p,r ORDER BY r.stripe WITH p,collect(r.token) AS tokens SET p.next_page=p.next_page+1,p.tokens=tokens RETURN true AS ok",json!({"org":batch.org_id,"id":batch.batch_id(),"page":page,"fingerprint":batch.fingerprint.0,"run":batch.batch.run_id,"kind":batch.batch.kind.label(),"index":batch.batch.index,"at":at.to_rfc3339(),"stripe":batch.batch_id().as_bytes()[0]%64,"token":page_id(batch.batch_id(),page)}))
}
/// Store page boundaries, not just the input: resuming must never recompute
/// already committed boundaries using a different planner implementation.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct FrozenPlan {
    pub version: u32,
    pub parent: MutationBatch,
    pub pages: Vec<MutationBatch>,
}
/// Encode the already validated page boundaries produced by core admission.
pub fn encode(
    batch: &MutationBatch,
    pages: Vec<MutationBatch>,
) -> Result<(Uuid, Vec<String>), BackendError> {
    let mut parent = batch.clone();
    parent.mutations.clear();
    parent.preconditions.clear();
    let plan = FrozenPlan {
        version: 1,
        parent,
        pages,
    };
    let body =
        serde_json::to_string(&plan).map_err(|e| BackendError::Serialization(e.to_string()))?;
    if body.len() > MAX_PLAN_BYTES {
        return Err(BackendError::Query(
            "frozen commit plan exceeds 64 MiB".into(),
        ));
    }
    let digest = Uuid::new_v5(&batch.batch_id(), body.as_bytes());
    let mut start = 0;
    let mut parts = vec![];
    while start < body.len() {
        let mut end = (start + PART_BYTES).min(body.len());
        while !body.is_char_boundary(end) {
            end -= 1;
        }
        parts.push(body[start..end].to_string());
        start = end;
    }
    Ok((digest, parts))
}
