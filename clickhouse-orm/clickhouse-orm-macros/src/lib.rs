use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

mod adapters;
mod domain;
mod generator;

use crate::adapters::head_options_macros::table_name::get_table_name;
use crate::domain::engine_parser::EngineParser;
use crate::domain::table_options::TableOptions;
use domain::engine_config::EngineConfig;
use generator::sql_generator::generate_create_table_sql;

#[proc_macro_derive(
    ClickHouseTable,
    attributes(table_name, table_engine, table_engine_options, table_options)
)]
pub fn clickhouse_table_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let table_name_str = get_table_name(&input);
    let table_options = TableOptions::from_derive_input(&input);
    let engine_config = EngineConfig::from_attributes(&input.attrs);

    let mut merged_options = table_options.clone();
    merged_options.engine = Some(engine_config.engine_type.clone());

    let create_sql_str = generate_create_table_sql(&input, &table_name_str, &merged_options);

    let table_name = syn::LitStr::new(&table_name_str, proc_macro2::Span::call_site());
    let create_sql = syn::LitStr::new(&create_sql_str, proc_macro2::Span::call_site());
    let engine_expr = EngineParser::parse_engine(&engine_config);
    let flag_type = EngineParser::get_flag_type(&engine_config);

    let expanded = quote! {
        impl<'a> #impl_generics ::std::convert::From<&'a #name #ty_generics>
            for <#name #ty_generics as clickhouse_orm::clickhouse::Row>::Value<'a>
        #where_clause
        {
            fn from(value: &'a #name #ty_generics) -> Self {
                value.clone()
            }
        }

        impl #impl_generics clickhouse_orm::ClickHouseTable for #name #ty_generics #where_clause {
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

        impl #impl_generics #name #ty_generics #where_clause {
            pub fn repository(client: clickhouse_orm::CHClient) -> clickhouse_orm::Repository<Self, #flag_type> {
                clickhouse_orm::Repository::new(client, Self::table_name(), #engine_expr)
            }
        }
    };

    TokenStream::from(expanded)
}
