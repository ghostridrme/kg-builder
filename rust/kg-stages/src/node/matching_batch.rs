//! Bounded identity requests sharing only evidence already visible to every component.

use super::{
    fuzzy_match::{invalid, STAGE},
    matching_candidates::FuzzyCandidate,
    matching_decision::{self, Decision, LocalCandidate, PreparedDecision},
};
use kg_core::{
    errors::{stage::ModelFailureKind, StageError},
    models::EntityNode,
    runtime::{
        matching_cache::{CacheKey, CacheWrite},
        stage_output::NodeIdentityOutput,
        RuntimeContext,
    },
    traits::{
        llm_backend::{LlmMessage, MessageRole},
        IdentityRevision,
    },
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

pub(super) struct Request<'a> {
    pub id: Uuid,
    pub members: &'a [(usize, EntityNode)],
    pub candidates: &'a [FuzzyCandidate],
    pub locals: &'a [LocalCandidate],
    pub revision: &'a IdentityRevision,
    prepared: PreparedDecision,
}

impl<'a> Request<'a> {
    /// The frozen evidence this component is decided on.
    pub fn evidence(&self) -> &Value {
        &self.prepared.evidence
    }

    /// True for text mentions without authoritative keys.
    pub fn requires_evidence(&self) -> bool {
        self.prepared.requires_evidence
    }

    pub fn new(
        id: Uuid,
        members: &'a [(usize, EntityNode)],
        candidates: &'a [FuzzyCandidate],
        locals: &'a [LocalCandidate],
        revision: &'a IdentityRevision,
        outputs: &[NodeIdentityOutput],
        ctx: &RuntimeContext,
    ) -> Result<Self, StageError> {
        let prepared =
            matching_decision::prepare(members, outputs, candidates, locals, revision, ctx)?;
        if prepared.evidence["component_id"] != json!(id) {
            return Err(invalid(
                "identity batch component ID disagrees with observation",
            ));
        }
        Ok(Self {
            id,
            members,
            candidates,
            locals,
            revision,
            prepared,
        })
    }
}

pub(super) const BATCH_PROMPT: &str = "Apply the identity decision rules independently to every component in components. Evidence is losslessly shared: observations indexes the observations table; each observation's context indexes contexts containing its snapshot and permitted history; each context schemas field indexes the schemas table. Candidate record indexes records; incoming candidate observations indexes observations. candidate_id remains local to that component's candidates list, never a table index or another component's candidate list. Shared tables do not grant new candidates or permission to use another observation's history. Return {decisions:[{component_id,decision}]} with exactly one entry per requested component_id, no missing or duplicate entries. decision has the outcome, candidate_id and, when required by the schema, evidence fields described above. Decide all components against the frozen evidence; do not invent new candidates from another decision. A local match may refer to a component decided in a different request; the caller validates all links together.";

fn observation_context(observation: &Value) -> Value {
    json!({"snapshot":observation["snapshot"], "history":observation["history"], "schemas":observation["schemas"]})
}

/// Equal footprints prevent batching from exposing new histories or candidate facts.
fn visibility(prepared: &PreparedDecision) -> Value {
    let mut observations = BTreeSet::new();
    let mut records = BTreeSet::new();
    let mut current = BTreeSet::new();
    for observation in prepared.evidence["observations"]
        .as_array()
        .into_iter()
        .flatten()
    {
        observations.insert(observation.to_string());
        current.insert(observation_context(observation).to_string());
    }
    for candidate in prepared.evidence["candidates"]
        .as_array()
        .into_iter()
        .flatten()
    {
        for observation in candidate["observations"].as_array().into_iter().flatten() {
            observations.insert(observation.to_string());
        }
        for key in ["record", "stored"] {
            if !candidate[key].is_null() {
                records.insert(candidate[key].to_string());
            }
        }
    }
    json!({"revision":prepared.revision,"system":prepared.messages[0].content,
        "schema":prepared.schema,"current":current,"observations":observations,"records":records})
}

#[derive(Default)]
struct Table {
    values: Vec<Value>,
    ids: BTreeMap<String, usize>,
}
impl Table {
    fn intern(&mut self, value: Value) -> usize {
        let key = value.to_string();
        if let Some(id) = self.ids.get(&key) {
            return *id;
        }
        let id = self.values.len();
        self.values.push(value);
        self.ids.insert(key, id);
        id
    }
}

fn observation_refs(
    values: &Value,
    observations: &mut Table,
    contexts: &mut Table,
    schemas: &mut Table,
) -> Vec<usize> {
    values
        .as_array()
        .into_iter()
        .flatten()
        .map(|value| {
            let mut context_value = observation_context(value);
            context_value["schemas"] = json!(schemas.intern(context_value["schemas"].clone()));
            let context = contexts.intern(context_value);
            let mut observation = value.clone();
            let object = observation
                .as_object_mut()
                .expect("validated identity observation");
            for key in ["snapshot", "history", "schemas"] {
                object.remove(key);
            }
            object.insert("context".into(), json!(context));
            observations.intern(observation)
        })
        .collect()
}

struct Encoded {
    evidence: Value,
    messages: Vec<LlmMessage>,
    schema: Value,
    output_tokens: u32,
}

pub(super) fn shared_evidence<'a>(evidence_rows: impl IntoIterator<Item = &'a Value>) -> Value {
    let mut contexts = Table::default();
    let mut observations = Table::default();
    let mut records = Table::default();
    let mut schemas = Table::default();
    let mut components = Vec::new();
    for evidence in evidence_rows {
        let observation_ids = observation_refs(
            &evidence["observations"],
            &mut observations,
            &mut contexts,
            &mut schemas,
        );
        let mut candidates = Vec::new();
        for original in evidence["candidates"].as_array().into_iter().flatten() {
            let mut candidate = original.clone();
            if candidate["observations"].is_array() {
                candidate["observations"] = json!(observation_refs(
                    &original["observations"],
                    &mut observations,
                    &mut contexts,
                    &mut schemas
                ));
            }
            for key in ["record", "stored"] {
                if !candidate[key].is_null() {
                    candidate[key] = json!(records.intern(original[key].clone()));
                }
            }
            candidates.push(candidate);
        }
        components.push(json!({"component_id":evidence["component_id"],"observations":observation_ids,"candidates":candidates}));
    }
    json!({"components":components,"contexts":contexts.values,
        "observations":observations.values,"records":records.values,"schemas":schemas.values})
}

fn encode(requests: &[Request<'_>], ctx: &RuntimeContext) -> Result<Encoded, StageError> {
    let first = requests
        .first()
        .ok_or_else(|| invalid("empty identity batch"))?;
    let evidence = shared_evidence(requests.iter().map(|request| &request.prepared.evidence));
    let messages = vec![
        LlmMessage {
            role: MessageRole::System,
            content: format!("{}\n{}", first.prepared.messages[0].content, BATCH_PROMPT),
        },
        LlmMessage {
            role: MessageRole::User,
            content: kg_core::sanitize::fence_untrusted(&evidence.to_string()),
        },
    ];
    // No per-request identifiers in the schema (see the single-request schema):
    // `parse_entries` requires exactly one answer per requested component id.
    let decision_schema = first.prepared.schema.clone();
    let schema = json!({"type":"object","additionalProperties":false,"required":["decisions"],"properties":{
        "decisions":{"type":"array","items":{"type":"object","additionalProperties":false,
            "required":["component_id","decision"],"properties":{
                "component_id":{"type":"string"},"decision":decision_schema}}}}});
    if requests.len() > ctx.matching_settings.max_batch_components
        || ctx
            .matching_settings
            .max_output_tokens
            .saturating_mul(requests.len() as u32)
            > ctx.matching_settings.max_batch_output_tokens
    {
        return Err(invalid("identity batch exceeds output reservation"));
    }
    let output_tokens = ctx
        .matching_settings
        .max_output_tokens
        .saturating_mul(requests.len() as u32)
        .min(ctx.matching_settings.max_batch_output_tokens);
    if messages.iter().map(|m| m.content.len()).sum::<usize>()
        > ctx.matching_settings.max_prompt_bytes
    {
        return Err(invalid("identity batch exceeds prompt byte budget"));
    }
    // JSON transport escapes source quotes/newlines again. Packing must include
    // those bytes and per-message framing, not only the unescaped prompt text.
    let wire_bytes = serde_json::to_vec(&json!({"messages": messages, "response_format": schema}))
        .map_err(|_| invalid("cannot encode identity request budget"))?
        .len();
    let framing = 4096usize.saturating_mul(messages.len().saturating_add(1));
    if wire_bytes
        .saturating_add(framing)
        .saturating_add(output_tokens as usize)
        > ctx.llm_disambiguation.context_window()
    {
        return Err(invalid("identity batch exceeds transport context budget"));
    }
    kg_core::runtime::history::check_prompt_budget(
        STAGE,
        ctx.llm_disambiguation.as_ref(),
        &messages,
        &schema,
        output_tokens,
    )?;
    Ok(Encoded {
        evidence,
        messages,
        schema,
        output_tokens,
    })
}

pub(super) fn pack<'a>(
    mut requests: Vec<Request<'a>>,
    ctx: &RuntimeContext,
) -> Result<Vec<Vec<Request<'a>>>, StageError> {
    ctx.matching_settings
        .validate()
        .map_err(|_| invalid("invalid matching settings"))?;
    requests.sort_by_key(|r| r.id);
    if requests.windows(2).any(|pair| pair[0].id == pair[1].id) {
        return Err(invalid("duplicate identity batch component"));
    }
    let mut groups = BTreeMap::<String, Vec<Request<'a>>>::new();
    for request in requests {
        groups
            .entry(visibility(&request.prepared).to_string())
            .or_default()
            .push(request);
    }
    let mut batches = Vec::new();
    for group in groups.into_values() {
        let mut batch = Vec::new();
        for request in group {
            if batch.len() == ctx.matching_settings.max_batch_components {
                batches.push(std::mem::take(&mut batch));
            }
            batch.push(request);
            if batch.len() > 1 && encode(&batch, ctx).is_err() {
                let last = batch.pop().expect("nonempty batch");
                batches.push(std::mem::take(&mut batch));
                batch.push(last);
            }
        }
        if !batch.is_empty() {
            batches.push(batch);
        }
    }
    tracing::debug!(
        components = batches.iter().map(Vec::len).sum::<usize>(),
        batches = batches.len(),
        "identity requests packed"
    );
    Ok(batches)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Response {
    decisions: Vec<Entry>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    component_id: Uuid,
    decision: Value,
}

fn parse_entries(
    text: &str,
    limit: usize,
    ids: &[Uuid],
) -> Result<BTreeMap<Uuid, Value>, StageError> {
    let failure = || StageError::ModelCall {
        stage: "fuzzy_match".into(),
        kind: ModelFailureKind::InvalidResponse,
    };
    let value = crate::model_output::parse_json(text, limit).map_err(|_| failure())?;
    let response: Response = serde_json::from_value(value).map_err(|_| failure())?;
    if response.decisions.len() != ids.len() {
        return Err(failure());
    }
    let mut result = BTreeMap::new();
    for entry in response.decisions {
        if !ids.contains(&entry.component_id)
            || result.insert(entry.component_id, entry.decision).is_some()
        {
            return Err(failure());
        }
    }
    Ok(result)
}

pub(super) async fn decide(
    requests: Vec<Request<'_>>,
    outputs: &[NodeIdentityOutput],
    ctx: &RuntimeContext,
) -> Result<Vec<(Uuid, Decision)>, StageError> {
    if ctx.cancel.is_cancelled() {
        return Err(StageError::Cancelled {
            stage: "fuzzy_match".into(),
        });
    }
    if requests.len() == 1 {
        let r = &requests[0];
        return Ok(vec![(
            r.id,
            matching_decision::decide(r.members, outputs, r.candidates, r.locals, r.revision, ctx)
                .await?,
        )]);
    }
    let encoded = encode(&requests, ctx)?;
    let keys: Vec<_> = requests.iter().map(|r| CacheKey::from_value(&json!({
        "version":"identity-batch-v1","org_id":ctx.org_id,"component_id":r.id,
        "evidence":encoded.evidence,"system":encoded.messages[0].content,"schema":encoded.schema,
        "revision":r.revision,"model":ctx.llm_disambiguation.processing_descriptor(),"settings":ctx.matching_settings,
        "typed":ctx.typed_decisions,"decisions":ctx.decisions.as_ref().map(|d| d.processing_descriptor()),
    }))).collect();
    // Partial hits cannot shrink the prompt: every decision depends on the full visible batch.
    let cached: Option<Vec<_>> = requests
        .iter()
        .zip(&keys)
        .map(|(r, key)| {
            let hit = ctx.matching_cache.get(key)?;
            let candidate_id = r.candidates.iter().position(|c| {
                c.record.chain_id == hit.chain_id && c.record.uuid == hit.version_uuid
            })?;
            Some((
                r.id,
                Decision::Match {
                    candidate_id,
                    resolved_by: "fuzzy_llm:cached".into(),
                    cache_write: None,
                },
            ))
        })
        .collect();
    if let Some(cached) = cached {
        return Ok(cached);
    }
    let response = super::extraction_support::call_provider(
        ctx,
        "fuzzy_match",
        &encoded.messages,
        &encoded.schema,
        ctx.llm_disambiguation.as_ref(),
        &ctx.llm_disambiguation_semaphore,
        ctx.matching_settings.timeout_ms,
        encoded.output_tokens,
    )
    .await?;
    let ids: Vec<_> = requests.iter().map(|r| r.id).collect();
    let entries = parse_entries(
        &response.content,
        ctx.matching_settings.max_batch_response_bytes,
        &ids,
    )?;
    let mut decisions = Vec::new();
    for (r, key) in requests.iter().zip(keys) {
        let text = entries[&r.id].to_string();
        let parsed = if r.prepared.requires_evidence {
            matching_decision::parse_keyless(
                &text,
                ctx.matching_settings.max_response_bytes,
                r.candidates.len() + r.locals.len(),
                &response.model,
                r.members,
                outputs,
            )
        } else {
            matching_decision::parse(
                &text,
                ctx.matching_settings.max_response_bytes,
                r.candidates.len() + r.locals.len(),
                &response.model,
            )
        };
        let mut decision =
            matching_decision::partial_decision(parsed, ctx.exec_config.continue_on_step_error)?;
        if let Decision::Match {
            candidate_id,
            cache_write,
            ..
        } = &mut decision
        {
            if let Some(candidate) = r.candidates.get(*candidate_id) {
                *cache_write = Some(CacheWrite {
                    key,
                    chain_id: candidate.record.chain_id,
                    version_uuid: candidate.record.uuid,
                });
            }
        }
        decisions.push((r.id, decision));
    }
    tracing::debug!(
        components = requests.len(),
        input_tokens = response.input_tokens,
        output_tokens = response.output_tokens,
        prompt_bytes = encoded
            .messages
            .iter()
            .map(|m| m.content.len())
            .sum::<usize>(),
        "identity batch validated"
    );
    Ok(decisions)
}
