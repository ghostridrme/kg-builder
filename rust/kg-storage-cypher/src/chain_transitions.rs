//! Merge intervals preserve source history. The state lock also covers fuzzy
//! adoptions whose losing chain has no entity node. Winner locks serialize alias
//! grants; splitting releases only aliases owned solely by the closed grant.

pub(crate) const LOCK_STATE: &str =
    "UNWIND [{scope:$state,chain:$loser},{scope:$winner_state,chain:$winner}] AS row
    WITH row ORDER BY row.scope
    MERGE (state:ChainMergeState {scope_id:row.scope})
    ON CREATE SET state.org_id=$org,state.chain_id=row.chain
    SET state.scope_id=state.scope_id
    RETURN count(state)=2 AS ok";

pub(crate) const LOCK_WINNER: &str = "MATCH (winner:Entity {org_id:$org,chain_id:$winner,is_latest:true})
    SET winner.uuid=winner.uuid
    WITH winner WHERE winner.is_latest=true AND winner.deleted_at IS NULL AND winner.merged_into IS NULL
    RETURN true AS ok";

pub(crate) const LOCK_LOSER: &str = "MATCH (n:Entity {org_id:$org,chain_id:$loser})
    SET n.uuid=n.uuid
    RETURN count(*) >= 0 AS ok";

pub(crate) const CHECK_MERGE: &str = "MATCH (state:ChainMergeState {scope_id:$state}), (winner_state:ChainMergeState {scope_id:$winner_state})
    MATCH (winner:Entity {org_id:$org,chain_id:$winner,is_latest:true})
    OPTIONAL MATCH (loser:Entity {chain_id:$loser})
    WITH state,winner_state,winner,collect(loser) AS losers
    WHERE (state.watermark IS NULL OR datetime(state.watermark)<=datetime($at))
      AND (winner_state.watermark IS NULL OR datetime(winner_state.watermark)<=datetime($at))
      AND (state.action IS NULL OR
        (state.action='split' AND datetime(state.at)<datetime($at)) OR
        (state.action='merge' AND state.at=$at AND state.winner=$winner AND state.hashes=$hashes))
      AND all(n IN losers WHERE n.org_id=$org
        AND coalesce(n.namespace,'')=coalesce(winner.namespace,'')
        AND (n.merged_into IS NULL OR n.merged_into=$winner)
        AND (n.valid_from IS NULL OR datetime(n.valid_from)<=datetime($at))
        AND (n.last_seen_at IS NULL OR datetime(n.last_seen_at)<=datetime($at))
        AND (n.deleted_at IS NULL OR datetime(n.deleted_at)<=datetime($at)))
      AND EXISTS { MATCH (v:Entity {org_id:$org,chain_id:$winner})
        WHERE (v.valid_from IS NULL OR datetime(v.valid_from)<=datetime($at))
          AND (v.valid_to IS NULL OR datetime($at)<datetime(v.valid_to))
          AND (v.deleted_at IS NULL OR datetime($at)<datetime(v.deleted_at)) }
      AND NOT EXISTS { MATCH (m:ChainMerge {org_id:$org,loser_chain_id:$winner})
        WHERE datetime(m.valid_from)<=datetime($at) AND (m.valid_to IS NULL OR datetime($at)<datetime(m.valid_to)) }
      AND NOT EXISTS { MATCH (m:ChainMerge {org_id:$org,winner_chain_id:$loser}) WHERE m.valid_to IS NULL }
    RETURN true AS ok";

// Lock physical incident facts as well: live reads deliberately hide orphan
// edges, but those must not reappear if their other endpoint is restored later.
pub(crate) const CLOSE_FACTS: &str = "MATCH (n:Entity {org_id:$org,chain_id:$loser})
    MATCH (n)-[r:RELATES_TO {org_id:$org}]-(other:Entity {org_id:$org})
    WITH DISTINCT r
    SET r.uuid=r.uuid
    WITH collect(r) AS edges
    WITH [r IN edges WHERE r.cancelled_at IS NULL AND r.deleted_at IS NULL
      AND (r.valid_to IS NULL OR r.valid_from IS NULL OR datetime(r.valid_from)<datetime(r.valid_to))
      AND (r.invalid_at IS NULL OR r.valid_from IS NULL OR datetime(r.valid_from)<datetime(r.invalid_at))
      AND (r.invalid_at IS NULL OR datetime($at)<datetime(r.invalid_at))
      AND (r.valid_to IS NULL OR datetime($at)<datetime(r.valid_to))] AS changed
    WHERE all(r IN changed WHERE
      (r.last_seen_at IS NULL OR datetime(r.last_seen_at)<=datetime($at)) AND
      (r.last_transition_at IS NULL OR datetime(r.last_transition_at)<=datetime($at)))
    WITH [r IN changed WHERE r.valid_from IS NULL OR datetime(r.valid_from)<=datetime($at)] AS active,
      [r IN changed WHERE datetime($at)<datetime(r.valid_from)] AS pending
    FOREACH (r IN active | SET r.is_latest=false,r.invalid_at=$at,r.last_transition_at=$at)
    FOREACH (r IN pending | SET r.is_latest=false,r.cancelled_at=$at,
      r.cancellation_context=$cancellation_context,r.last_transition_at=$at REMOVE r.cancellation_snapshot_id)
    RETURN true AS ok";

pub(crate) const GRANT_ALIASES: &str = "MATCH (state:ChainMergeState {scope_id:$state}), (winner_state:ChainMergeState {scope_id:$winner_state})
    MATCH (winner:Entity {org_id:$org,chain_id:$winner,is_latest:true})
    OPTIONAL MATCH (grant:ChainMerge {org_id:$org,winner_chain_id:$winner}) WHERE grant.valid_to IS NULL
    WITH state,winner_state,winner,collect(grant) AS grants
    MERGE (period:ChainMerge {period_id:$period})
    ON CREATE SET period.org_id=$org,period.loser_chain_id=$loser,period.winner_chain_id=$winner,
        period.valid_from=$at,period.hashes=$hashes,
        period.managed_hashes=[h IN $hashes WHERE NOT h IN coalesce(winner.identity_hashes,[]) OR any(g IN grants WHERE h IN g.managed_hashes)]
    SET winner.identity_hashes=[h IN coalesce(winner.identity_hashes,[]) WHERE NOT h IN $hashes]+$hashes,
        state.action='merge',state.at=$at,state.winner=$winner,state.hashes=$hashes,state.period=$period,
        state.watermark=$at,winner_state.watermark=$at,winner.last_transition_at=$at
    RETURN true AS ok";

pub(crate) const HIDE_LOSER: &str = "MATCH (n:Entity {org_id:$org,chain_id:$loser})
    SET n.merged_into=$winner,n.is_latest=false,n.last_transition_at=$at
    RETURN count(*) >= 0 AS ok";

pub(crate) const CHECK_SPLIT: &str = "MATCH (state:ChainMergeState {scope_id:$state}), (winner_state:ChainMergeState {scope_id:$winner_state})
    MATCH (period:ChainMerge {period_id:state.period})
    MATCH (n:Entity {org_id:$org,chain_id:$loser})
    WITH state,winner_state,period,collect(n) AS versions
    WHERE (state.watermark IS NULL OR datetime(state.watermark)<=datetime($at))
      AND (winner_state.watermark IS NULL OR datetime(winner_state.watermark)<=datetime($at))
      AND state.winner=$winner AND state.hashes=$hashes AND (
        (state.action='merge' AND datetime(state.at)<datetime($at) AND period.valid_to IS NULL
            AND all(n IN versions WHERE n.merged_into=$winner)) OR
        (state.action='split' AND state.at=$at AND period.valid_to=$at))
    RETURN true AS ok";

pub(crate) const RELEASE_ALIASES: &str = "MATCH (state:ChainMergeState {scope_id:$state}), (winner_state:ChainMergeState {scope_id:$winner_state})
    MATCH (period:ChainMerge {period_id:state.period})
    MATCH (winner:Entity {org_id:$org,chain_id:$winner,is_latest:true})
    OPTIONAL MATCH (native:Entity {org_id:$org,chain_id:$winner})
    WITH state,winner_state,period,winner,collect(native.identity_hash) AS native_hashes
    SET period.valid_to=$at
    WITH state,winner_state,period,winner,native_hashes
    OPTIONAL MATCH (grant:ChainMerge {org_id:$org,winner_chain_id:$winner}) WHERE grant.valid_to IS NULL
    WITH state,winner_state,period,winner,native_hashes,collect(grant) AS remaining
    SET winner.identity_hashes=[h IN coalesce(winner.identity_hashes,[]) WHERE
        NOT h IN period.managed_hashes OR h IN native_hashes OR any(g IN remaining WHERE h IN g.hashes)],
        state.action='split',state.at=$at,
        state.watermark=$at,winner_state.watermark=$at,winner.last_transition_at=$at
    RETURN true AS ok";

pub(crate) const RESTORE_LOSER: &str = "MATCH (n:Entity {org_id:$org,chain_id:$loser})
    REMOVE n.merged_into
    SET n.is_latest=false,n.last_transition_at=$at
    WITH n ORDER BY n.version DESC,n.uuid LIMIT 1
    SET n.is_latest=(n.deleted_at IS NULL AND n.valid_to IS NULL)
    RETURN true AS ok";
