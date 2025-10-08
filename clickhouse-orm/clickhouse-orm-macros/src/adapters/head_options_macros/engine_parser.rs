use super::engine_config::EngineConfig;
use proc_macro2::TokenStream;
use quote::quote;

pub struct EngineParser;

impl EngineParser {
    pub fn parse_engine(config: &EngineConfig) -> TokenStream {
        match config.engine_type.as_str() {
            "ReplicatedMergeTree" => Self::parse_replicated_merge_tree(config),
            "CollapsingMergeTree" => Self::parse_collapsing_merge_tree(config),
            "VersionedCollapsingMergeTree" => Self::parse_versioned_collapsing_merge_tree(config),
            "SummingMergeTree" => Self::parse_summing_merge_tree(config),
            "AggregatingMergeTree" => quote! { clickhouse_orm::Engine::AggregatingMergeTree },
            "ReplacingMergeTree" => quote! { clickhouse_orm::Engine::ReplacingMergeTree },
            "GraphiteMergeTree" => quote! { clickhouse_orm::Engine::GraphiteMergeTree },
            "Log" => quote! { clickhouse_orm::Engine::Log },
            "TinyLog" => quote! { clickhouse_orm::Engine::TinyLog },
            "Memory" => quote! { clickhouse_orm::Engine::Memory },
            "Buffer" => quote! { clickhouse_orm::Engine::Buffer },
            "Distributed" => quote! { clickhouse_orm::Engine::Distributed },
            "MergeTree" | _ => quote! { clickhouse_orm::Engine::MergeTree },
        }
    }

    fn parse_replicated_merge_tree(config: &EngineConfig) -> TokenStream {
        let zk_path = config.zk_path.as_deref().unwrap_or("/clickhouse/tables/{shard}/default");
        let replica = config.replica.as_deref().unwrap_or("{replica}");

        quote! {
            clickhouse_orm::Engine::ReplicatedMergeTree {
                zk_path: #zk_path.to_string(),
                replica: #replica.to_string(),
            }
        }
    }

    fn parse_collapsing_merge_tree(config: &EngineConfig) -> TokenStream {
        let sign_column = config.sign_column.as_deref().unwrap_or("sign");

        quote! {
            clickhouse_orm::Engine::CollapsingMergeTree {
                sign_column: #sign_column.to_string(),
            }
        }
    }

    fn parse_versioned_collapsing_merge_tree(config: &EngineConfig) -> TokenStream {
        let sign_column = config.sign_column.as_deref().unwrap_or("sign");
        let version_column = config.version_column.as_deref().unwrap_or("version");

        quote! {
            clickhouse_orm::Engine::VersionedCollapsingMergeTree {
                sign_column: #sign_column.to_string(),
                version_column: #version_column.to_string(),
            }
        }
    }

    fn parse_summing_merge_tree(config: &EngineConfig) -> TokenStream {
        if let Some(columns) = &config.columns {
            let cols = columns.iter().map(|c| c.as_str());
            quote! {
                clickhouse_orm::Engine::SummingMergeTree {
                    columns: vec![#(#cols.to_string()),*],
                }
            }
        } else {
            quote! { clickhouse_orm::Engine::SummingMergeTree { columns: vec![] } }
        }
    }
}
