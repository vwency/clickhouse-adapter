use crate::domain::engine_config::{EngineConfig, EngineType};
use crate::domain::engine_parser::EngineParser;
use proc_macro2::TokenStream;
use quote::quote;

impl EngineParser {
    pub fn parse_engine(config: &EngineConfig) -> TokenStream {
        match &config.engine_type {
            EngineType::ReplicatedMergeTree => Self::replicated_merge_tree(config),
            EngineType::CollapsingMergeTree => Self::collapsing_merge_tree(config),
            EngineType::VersionedCollapsingMergeTree => {
                Self::versioned_collapsing_merge_tree(config)
            }
            EngineType::SummingMergeTree => Self::summing_merge_tree(config),
            EngineType::AggregatingMergeTree => {
                quote! { clickhouse_orm::Engine::AggregatingMergeTree }
            }
            EngineType::ReplacingMergeTree => quote! { clickhouse_orm::Engine::ReplacingMergeTree },
            EngineType::GraphiteMergeTree => quote! { clickhouse_orm::Engine::GraphiteMergeTree },
            EngineType::Log => quote! { clickhouse_orm::Engine::Log },
            EngineType::TinyLog => quote! { clickhouse_orm::Engine::TinyLog },
            EngineType::Memory => quote! { clickhouse_orm::Engine::Memory },
            EngineType::Buffer => quote! { clickhouse_orm::Engine::Buffer },
            EngineType::Distributed => quote! { clickhouse_orm::Engine::Distributed },
            EngineType::MergeTree => quote! { clickhouse_orm::Engine::MergeTree },
            EngineType::Other(_) => quote! { clickhouse_orm::Engine::MergeTree }, // fallback
        }
    }

    pub fn get_flag_type(config: &EngineConfig) -> TokenStream {
        match &config.engine_type {
            EngineType::MergeTree => quote! { clickhouse_orm::MergeTreeFlag },
            EngineType::ReplicatedMergeTree => quote! { clickhouse_orm::ReplicatedMergeTreeFlag },
            EngineType::SummingMergeTree => quote! { clickhouse_orm::SummingMergeTreeFlag },
            EngineType::AggregatingMergeTree => quote! { clickhouse_orm::AggregatingMergeTreeFlag },
            EngineType::CollapsingMergeTree => quote! { clickhouse_orm::CollapsingMergeTreeFlag },
            EngineType::VersionedCollapsingMergeTree => {
                quote! { clickhouse_orm::VersionedCollapsingMergeTreeFlag }
            }
            EngineType::ReplacingMergeTree => quote! { clickhouse_orm::ReplacingMergeTreeFlag },
            EngineType::GraphiteMergeTree => quote! { clickhouse_orm::GraphiteMergeTreeFlag },
            EngineType::Log => quote! { clickhouse_orm::LogFlag },
            EngineType::TinyLog => quote! { clickhouse_orm::TinyLogFlag },
            EngineType::Memory => quote! { clickhouse_orm::MemoryFlag },
            EngineType::Buffer => quote! { clickhouse_orm::BufferFlag },
            EngineType::Distributed => quote! { clickhouse_orm::DistributedFlag },
            EngineType::Other(_) => quote! { () },
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
