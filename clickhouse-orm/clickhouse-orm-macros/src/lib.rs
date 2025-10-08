use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};
mod adapters;
mod generator;

use adapters::head_options_macros::table_name::get_table_name;
use adapters::head_options_macros::table_options::TableOptions;
use generator::sql_generator::generate_create_table_sql;

#[proc_macro_derive(ClickHouseTable, attributes(table_name, clickhouse))]
pub fn clickhouse_table_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let table_name = get_table_name(&input);
    let options = TableOptions::from_derive_input(&input);
    let create_sql = generate_create_table_sql(&input, &table_name, &options);

    let engine_value = options.engine.as_deref().unwrap_or("MergeTree");
    let engine_expr = if engine_value.starts_with("ReplicatedMergeTree") {
        quote! {
            clickhouse_orm::Engine::ReplicatedMergeTree {
                zk_path: String::new(),
                replica: String::new(),
            }
        }
    } else {
        match engine_value {
            "SummingMergeTree" => quote! { clickhouse_orm::Engine::SummingMergeTree },
            "AggregatingMergeTree" => quote! { clickhouse_orm::Engine::AggregatingMergeTree },
            "ReplacingMergeTree" => quote! { clickhouse_orm::Engine::ReplacingMergeTree },
            _ => quote! { clickhouse_orm::Engine::MergeTree },
        }
    };

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
            pub fn repository(client: clickhouse_orm::CHClient) -> clickhouse_orm::Repository<Self> {
                clickhouse_orm::Repository::new(client, Self::table_name())
            }
        }
    };

    TokenStream::from(expanded)
}
