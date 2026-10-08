//! Source-level architecture guards for crate boundaries and shared contracts.
//! Behavioral correctness is covered by the unit and integration tests.

use std::fs;
use std::path::{Path, PathBuf};

use syn::visit::Visit;

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn read(rel: &str) -> String {
    let path = workspace_root().join(rel);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

fn rust_sources(rel_dir: &str) -> Vec<(PathBuf, String)> {
    let mut out = Vec::new();
    let root = workspace_root().join(rel_dir);
    let mut stack = vec![root];
    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("cannot read directory {}: {e}", dir.display()));
        for entry in entries {
            let entry = entry.expect("read directory entry");
            let path = entry.path();
            if path.is_dir() {
                if path == workspace_root().join("rust/vendor") {
                    continue;
                }
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let content = fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
                out.push((path, content));
            }
        }
    }
    out
}

/// The `[dependencies]` table of a crate manifest, parsed rather than pattern-matched.
fn runtime_dependencies(manifest: &str) -> toml::Table {
    let parsed: toml::Table = manifest.parse().expect("manifest parses as TOML");
    parsed
        .get("dependencies")
        .and_then(|d| d.as_table())
        .cloned()
        .unwrap_or_default()
}

#[test]
fn secret_bearing_configs_do_not_derive_serialize() {
    for (path, content) in rust_sources("rust/kg-core/src/config") {
        if !content.contains("SecretString") {
            continue;
        }
        for item in content.split("#[derive(") {
            let Some((derives, rest)) = item.split_once(')') else {
                continue;
            };
            // Only flag derives directly above an item that contains a SecretString field.
            let item_head: String = rest.chars().take(400).collect();
            if item_head.contains("SecretString") {
                assert!(
                    !derives.contains("Serialize"),
                    "{}: type with SecretString field derives Serialize",
                    path.display()
                );
            }
        }
    }
}

#[test]
// Source hint only: this does not prove permits cover each call.
fn llm_call_sources_reference_semaphore() {
    let mut checked = 0;
    for (path, content) in rust_sources("rust/kg-stages/src") {
        // Mock-provider forwarding in dedicated test modules is not a production call site.
        if path.components().any(|part| part.as_os_str() == "tests")
            || path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "tests.rs")
        {
            continue;
        }
        if content.contains(".complete(") || content.contains("complete_as") {
            checked += 1;
            assert!(
                content.contains("semaphore"),
                "{}: LLM call source does not reference a semaphore",
                path.display()
            );
        }
    }
    assert!(checked > 0, "expected at least one LLM call site in stages");
}

#[test]
fn connector_entity_raw_properties_is_raw_json() {
    let content = read("rust/kg-core/src/models/input.rs");
    assert!(
        content.contains("pub raw_properties: serde_json::Value"),
        "ConnectorEntity.raw_properties must remain serde_json::Value"
    );
}

/// The removed event, queue, projector, and message-bus designs stay removed.
#[test]
fn core_has_no_event_or_queue_surfaces() {
    for rel in [
        "rust/kg-core/src/traits",
        "rust/kg-core/src/config",
        "rust/kg-core/src/errors",
    ] {
        for (path, content) in rust_sources(rel) {
            for forbidden in [
                "trait EventStore",
                "trait KnowledgeSink",
                "trait RunQueue",
                "trait ProjectorCheckpointStore",
                "trait ChainClaimStore",
                "EventStoreConfig",
                "EventStoreError",
                "KafkaIngestConfig",
                "KafkaPublisherConfig",
                "NatsJetstream",
                "dead_letter",
            ] {
                assert!(
                    !content.contains(forbidden),
                    "{}: found `{forbidden}`",
                    path.display()
                );
            }
        }
    }
    for rel in ["rust/kg-core/src/events", "rust/kg-core/src/memory.rs"] {
        assert!(
            !workspace_root().join(rel).exists(),
            "{rel} was removed with the event store"
        );
    }
}

#[test]
fn internal_dependency_matrix_is_the_allowlist() {
    // Checks plain [dependencies] entries; test dependencies may compose adapters.
    let allowed: &[(&str, &[&str])] = &[
        ("kg-core", &[]),
        ("kg-pipeline", &[]),
        ("kg-rig", &[]),
        ("kg-stages", &["kg-pipeline"]),
        ("kg-storage-neo4j", &["kg-storage-cypher"]),
        ("kg-storage-cypher", &[]),
        ("kg-search", &[]),
        ("kg-python", &["kg-rig", "kg-stages", "kg-storage-neo4j"]),
        (
            "kg-server",
            &[
                "kg-storage-neo4j",
                "kg-rig",
                "kg-search",
                // On-demand Saga summaries run the ingestion engine's receipted commit path.
                "kg-stages",
            ],
        ),
        // Dev-only live-database helpers; consumed through [dev-dependencies] only.
        ("kg-neo4j-testkit", &["kg-storage-neo4j"]),
    ];

    let crates_dir = workspace_root().join("rust");
    for entry in fs::read_dir(&crates_dir).expect("crates dir") {
        let entry = entry.expect("read crate directory entry");
        let name = entry.file_name().to_string_lossy().to_string();
        if !entry.path().is_dir() || name.starts_with('.') || name == "vendor" {
            continue;
        }
        assert!(
            allowed.iter().any(|(k, _)| *k == name),
            "crate `{name}` needs an explicit dependency allowlist"
        );
    }

    for (krate, extra) in allowed {
        // Only the [dependencies] table — dev-dependencies are unrestricted.
        for dep in runtime_dependencies(&read(&format!("rust/{krate}/Cargo.toml"))).keys() {
            let dep = dep.as_str();
            if !dep.starts_with("kg-") || dep == "kg-core" {
                continue;
            }
            assert!(
                extra.contains(&dep),
                "{krate} depends on {dep}, which its dependency allowlist does not permit"
            );
            assert!(
                dep != "kg-neo4j-testkit",
                "{krate} lists the dev-only kg-neo4j-testkit under [dependencies]"
            );
        }
    }
}

/// Every `#[ignore]` reason is one of the five tier literals. The reason
/// documents intent; tier selection is by cargo target and dev-only feature,
/// never by matching this string.
#[test]
fn ignore_reasons_are_the_five_tier_literals() {
    const ALLOWED: &[&str] = &[
        "live: Neo4j",
        "eval: Neo4j, writes evidence",
        "paid: provider spend",
        "load: exclusive Neo4j",
        "schema: exclusive destructive Neo4j",
    ];
    let mut seen = 0;
    let mut offenders = Vec::new();
    for (path, content) in rust_sources("rust") {
        if path.components().any(|part| part.as_os_str() == "target") {
            continue;
        }
        for (index, line) in content.lines().enumerate() {
            let trimmed = line.trim();
            if !trimmed.starts_with("#[ignore") {
                continue;
            }
            seen += 1;
            let reason = trimmed
                .strip_prefix("#[ignore")
                .and_then(|rest| rest.strip_suffix(']'))
                .map(str::trim)
                .and_then(|rest| rest.strip_prefix('='))
                .map(str::trim)
                .and_then(|rest| rest.strip_prefix('"'))
                .and_then(|rest| rest.strip_suffix('"'));
            match reason {
                Some(reason) if ALLOWED.contains(&reason) => {}
                _ => offenders.push(format!("{}:{}: {trimmed}", path.display(), index + 1)),
            }
        }
    }
    assert!(seen > 0, "expected ignored live tests in the workspace");
    assert!(
        offenders.is_empty(),
        "#[ignore] reasons must be one of {ALLOWED:?}:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn core_has_no_infrastructure_dependencies() {
    let manifest = read("rust/kg-core/Cargo.toml");
    for forbidden in [
        "sqlx",
        "neo4rs",
        "axum",
        "qdrant",
        "rig-core",
        "reqwest",
        "hyper",
        "rdkafka",
        "jsonwebtoken",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "kg-core must stay infrastructure-free, found `{forbidden}`"
        );
    }
}

/// The in-memory graph adapter is gone: the suites run against Neo4j, and
/// the mock providers live behind core's `test-support` feature, which only
/// test dependencies may enable.
#[test]
fn memory_adapter_is_removed() {
    assert!(
        !workspace_root().join("rust/kg-storage-memory").exists(),
        "rust/kg-storage-memory was removed with the in-memory graph"
    );
    assert!(!read("Cargo.toml").contains("kg-storage-memory"));
    let crates_dir = workspace_root().join("rust");
    for entry in fs::read_dir(&crates_dir).expect("crates dir") {
        let entry = entry.expect("read crate directory entry");
        let name = entry.file_name().to_string_lossy().to_string();
        if !entry.path().is_dir() || name.starts_with('.') || name == "vendor" {
            continue;
        }
        let manifest_path = entry.path().join("Cargo.toml");
        let manifest = fs::read_to_string(&manifest_path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", manifest_path.display()));
        assert!(
            !manifest.contains("kg-storage-memory"),
            "{name} still references kg-storage-memory"
        );
        let enables_test_support = runtime_dependencies(&manifest)
            .get("kg-core")
            .and_then(|core| core.get("features"))
            .and_then(|features| features.as_array())
            .is_some_and(|features| features.iter().any(|f| f.as_str() == Some("test-support")));
        assert!(
            !enables_test_support,
            "{name} enables core's test-support feature outside its test dependencies"
        );
    }
}

// ---------------------------------------------------------------- query ownership
// Runtime Cypher statements belong to kg-storage-cypher; other crates may
// only hold fixtures (moved here from the cypher crate's own test binary).

struct QueryLiterals(Vec<String>);
impl<'ast> Visit<'ast> for QueryLiterals {
    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        if item.attrs.iter().any(|a| {
            a.path().is_ident("cfg")
                && a.parse_args::<syn::Path>()
                    .is_ok_and(|p| p.is_ident("test"))
        }) {
            return;
        }
        syn::visit::visit_item_mod(self, item);
    }
    fn visit_lit_str(&mut self, literal: &'ast syn::LitStr) {
        let value = literal.value();
        let text = value.trim_start();
        if [
            "MATCH (",
            "OPTIONAL MATCH (",
            "RETURN ",
            "UNWIND ",
            "CALL db.",
            "CALL dbms.",
            "SHOW CONSTRAINTS",
            "SHOW TRANSACTIONS",
            "SHOW VECTOR",
            "SHOW FULLTEXT",
            "CREATE CONSTRAINT",
            "CREATE INDEX",
            "CREATE VECTOR",
            "CREATE FULLTEXT",
            "CREATE (",
            "MERGE (",
            "DROP INDEX",
            "TERMINATE TRANSACTIONS",
        ]
        .iter()
        .any(|prefix| text.starts_with(prefix))
        {
            self.0.push(value);
        }
    }
    fn visit_macro(&mut self, item: &'ast syn::Macro) {
        // format! and json! bodies are token streams, not ordinary AST expressions.
        fn inspect(visitor: &mut QueryLiterals, tokens: proc_macro2::TokenStream) {
            for token in tokens {
                match token {
                    proc_macro2::TokenTree::Group(group) => inspect(visitor, group.stream()),
                    proc_macro2::TokenTree::Literal(literal) => {
                        if let Ok(string) = syn::parse_str::<syn::LitStr>(&literal.to_string()) {
                            visitor.visit_lit_str(&string);
                        }
                    }
                    _ => {}
                }
            }
        }
        inspect(self, item.tokens.clone());
    }
}

fn check_dir(dir: &Path) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            if path.file_name().unwrap() != "tests" {
                check_dir(&path);
            }
        } else if path.extension().is_some_and(|e| e == "rs")
            && !["tests.rs", "test_support.rs"]
                .contains(&path.file_name().unwrap().to_str().unwrap())
        {
            let source = std::fs::read_to_string(&path).unwrap();
            let file = syn::parse_file(&source).unwrap();
            let mut visitor = QueryLiterals(Vec::new());
            visitor.visit_file(&file);
            assert!(
                visitor.0.is_empty(),
                "runtime query literals outside query crate: {}",
                path.display()
            );
        }
    }
}

#[test]
fn runtime_query_ownership() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    for name in [
        "kg-core",
        "kg-pipeline",
        "kg-stages",
        "kg-search",
        "kg-python",
        "kg-storage-neo4j",
    ] {
        check_dir(&root.join(name).join("src"));
    }
}
