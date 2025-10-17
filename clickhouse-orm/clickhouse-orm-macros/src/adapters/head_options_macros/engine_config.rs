use crate::domain::engine_config::{EngineConfig, EngineType};
use std::str::FromStr;
use syn::{Attribute, Lit, Meta};

pub struct EngineConfigParser;

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            engine_type: EngineType::MergeTree,
            zk_path: None,
            replica: None,
            sign_column: None,
            version_column: None,
            columns: None,
        }
    }
}
impl FromStr for EngineType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use EngineType::*;
        Ok(match s {
            "MergeTree" => MergeTree,
            "ReplicatedMergeTree" => ReplicatedMergeTree,
            "SummingMergeTree" => SummingMergeTree,
            "AggregatingMergeTree" => AggregatingMergeTree,
            "CollapsingMergeTree" => CollapsingMergeTree,
            "VersionedCollapsingMergeTree" => VersionedCollapsingMergeTree,
            "ReplacingMergeTree" => ReplacingMergeTree,
            "GraphiteMergeTree" => GraphiteMergeTree,
            "Log" => Log,
            "TinyLog" => TinyLog,
            "Memory" => Memory,
            "Buffer" => Buffer,
            "Distributed" => Distributed,
            other => Other(other.to_string()),
        })
    }
}

impl EngineConfig {
    pub fn from_attributes(attrs: &[syn::Attribute]) -> Self {
        use crate::adapters::head_options_macros::engine_config::EngineConfigParser;
        EngineConfigParser::parse(attrs)
    }
}

impl EngineConfigParser {
    pub fn parse(attrs: &[Attribute]) -> EngineConfig {
        let mut config = EngineConfig::default();

        for attr in attrs {
            if attr.path().is_ident("table_engine") {
                if let Meta::NameValue(nv) = &attr.meta {
                    if let syn::Expr::Lit(expr_lit) = &nv.value {
                        if let Lit::Str(lit_str) = &expr_lit.lit {
                            if let Ok(engine) = EngineType::from_str(&lit_str.value()) {
                                config.engine_type = engine;
                            }
                        }
                    }
                }
            }
        }

        for attr in attrs {
            if attr.path().is_ident("table_engine_options") {
                if let Meta::List(meta_list) = &attr.meta {
                    let tokens_str = meta_list.tokens.to_string();
                    for pair in tokens_str.split(',') {
                        let pair = pair.trim();
                        if let Some((key, value)) = pair.split_once('=') {
                            let key = key.trim();
                            let value = value.trim().trim_matches('"');
                            match key {
                                "zk_path" => config.zk_path = Some(value.to_string()),
                                "replica" => config.replica = Some(value.to_string()),
                                "sign_column" => config.sign_column = Some(value.to_string()),
                                "version_column" => config.version_column = Some(value.to_string()),
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        config
    }
}
