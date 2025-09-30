use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

mod sql_generator;
mod table_name;
mod table_options;
mod type_mapping;

use sql_generator::generate_create_table_sql;
use table_name::get_table_name;
use table_options::TableOptions;

#[proc_macro_derive(ClickHouseTable, attributes(table_name, clickhouse))]
pub fn clickhouse_table_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let table_name = get_table_name(&input);
    let options = TableOptions::from_derive_input(&input);
    let create_sql = generate_create_table_sql(&input, &table_name, &options);

    let expanded = quote! {
        impl clickhouse_orm::ClickHouseTable for #name {
            fn table_name() -> &'static str {
                #table_name
            }
            fn create_table_sql() -> &'static str {
                #create_sql
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
