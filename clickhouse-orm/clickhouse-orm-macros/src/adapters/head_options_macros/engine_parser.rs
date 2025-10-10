use crate::domain::engine_config::EngineConfig;
use crate::domain::engine_parser::EngineParser;
use proc_macro2::TokenStream;
use quote::quote;

impl EngineParser {
    pub fn parse_engine(config: &EngineConfig) -> TokenStream {
        match config.engine_type.as_str() {
            "ReplicatedMergeTree" => Self::replicated_merge_tree(config),
            "CollapsingMergeTree" => Self::collapsing_merge_tree(config),
            "VersionedCollapsingMergeTree" => Self::versioned_collapsing_merge_tree(config),
            "SummingMergeTree" => Self::summing_merge_tree(config),
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

    pub fn get_flag_type(config: &EngineConfig) -> TokenStream {
        match config.engine_type.as_str() {
            "MergeTree" => quote! { clickhouse_orm::MergeTreeFlag },
            "ReplicatedMergeTree" => quote! { clickhouse_orm::ReplicatedMergeTreeFlag },
            "SummingMergeTree" => quote! { clickhouse_orm::SummingMergeTreeFlag },
            "AggregatingMergeTree" => quote! { clickhouse_orm::AggregatingMergeTreeFlag },
            "CollapsingMergeTree" => quote! { clickhouse_orm::CollapsingMergeTreeFlag },
            "VersionedCollapsingMergeTree" => {
                quote! { clickhouse_orm::VersionedCollapsingMergeTreeFlag }
            }
            "ReplacingMergeTree" => quote! { clickhouse_orm::ReplacingMergeTreeFlag },
            "GraphiteMergeTree" => quote! { clickhouse_orm::GraphiteMergeTreeFlag },
            "Log" => quote! { clickhouse_orm::LogFlag },
            "TinyLog" => quote! { clickhouse_orm::TinyLogFlag },
            "Memory" => quote! { clickhouse_orm::MemoryFlag },
            "Buffer" => quote! { clickhouse_orm::BufferFlag },
            "Distributed" => quote! { clickhouse_orm::DistributedFlag },
            _ => quote! { () },
        }
    }

    fn get_str<'a>(opt: &'a Option<String>, default: &'a str) -> &'a str {
        opt.as_deref().unwrap_or(default)
    }

    fn replicated_merge_tree(config: &EngineConfig) -> TokenStream {
        let zk_path = Self::get_str(&config.zk_path, "/clickhouse/tables/{shard}/default");
        let replica = Self::get_str(&config.replica, "{replica}");

        quote! {
            clickhouse_orm::Engine::ReplicatedMergeTree {
                zk_path: #zk_path.to_string(),
                replica: #replica.to_string(),
            }
        }
    }

    fn collapsing_merge_tree(config: &EngineConfig) -> TokenStream {
        let sign_column = Self::get_str(&config.sign_column, "sign");

        quote! {
            clickhouse_orm::Engine::CollapsingMergeTree {
                sign_column: #sign_column.to_string(),
            }
        }
    }

    fn versioned_collapsing_merge_tree(config: &EngineConfig) -> TokenStream {
        let sign_column = Self::get_str(&config.sign_column, "sign");
        let version_column = Self::get_str(&config.version_column, "version");

        quote! {
            clickhouse_orm::Engine::VersionedCollapsingMergeTree {
                sign_column: #sign_column.to_string(),
                version_column: #version_column.to_string(),
            }
        }
    }

    fn summing_merge_tree(config: &EngineConfig) -> TokenStream {
        let columns = config.columns.as_ref().map_or(vec![], |cols| cols.clone());
        let columns_iter = columns.iter().map(|c| c.as_str());

        quote! {
            clickhouse_orm::Engine::SummingMergeTree {
                columns: vec![#(#columns_iter.to_string()),*],
            }
        }
    }
}
