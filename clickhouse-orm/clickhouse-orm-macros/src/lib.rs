use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

mod adapters;
mod domain;
mod generator;

use adapters::head_options_macros::engine_parser::EngineParser;
use adapters::head_options_macros::table_name::get_table_name;
use adapters::head_options_macros::table_options::TableOptions;
use domain::engine_config::EngineConfig;
use generator::sql_generator::generate_create_table_sql;

#[proc_macro_derive(ClickHouseTable, attributes(table_name, clickhouse))]
pub fn clickhouse_table_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Получаем имя таблицы
    let table_name = get_table_name(&input);

    // Опции таблицы
    let options = TableOptions::from_derive_input(&input);

    // Генерация SQL CREATE TABLE
    let create_sql = generate_create_table_sql(&input, &table_name, &options);

    // Парсинг движка из атрибутов
    let engine_config = EngineConfig::from_attributes(&input.attrs);

    // ОТЛАДКА: Посмотрим что в engine_config
    eprintln!("DEBUG: engine_type = {:?}", engine_config.engine_type);

    let engine_expr = EngineParser::parse_engine(&engine_config);

    // Определяем тип-флаг на основе engine_config
    let flag_type = match engine_config.engine_type.as_str() {
        "MergeTree" => {
            eprintln!("DEBUG: Matched MergeTree!");
            quote! { clickhouse_orm::MergeTreeFlag }
        }
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
        other => {
            eprintln!("DEBUG: No match for engine_type = '{}'", other);
            quote! { () }
        }
    };

    eprintln!("DEBUG: flag_type = {}", flag_type.to_string());

    let expanded = quote! {
        impl clickhouse_orm::ClickHouseTable for #name {
            fn table_name() -> &'static str {
                #table_name
            }
            fn create_table_sql() -> &'static str {
                #create_sql
            }
            fn engine() -> clickhouse_orm::Engine {
                #engine_expr
            }
        }

        impl #name {
            pub fn repository(client: clickhouse_orm::CHClient) -> clickhouse_orm::Repository<Self, #flag_type> {
                clickhouse_orm::Repository::new(client, Self::table_name(), #engine_expr)
            }
        }
    };

    TokenStream::from(expanded)
}
