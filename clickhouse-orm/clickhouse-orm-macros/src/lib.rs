use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Lit, Meta};

#[proc_macro_derive(ClickHouseTable, attributes(table_name, clickhouse))]
pub fn clickhouse_table_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let table_name = get_table_name(&input);
    let expanded = quote! {
        impl clickhouse_orm::ClickHouseTable for #name {
            fn table_name() -> &'static str {
                #table_name
            }
            fn create_table_sql() -> &'static str {
                concat!("CREATE TABLE IF NOT EXISTS ", #table_name, " (id UInt64) ENGINE = MergeTree ORDER BY id")
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

fn get_table_name(input: &DeriveInput) -> String {
    // Ищем атрибут #[table_name = "..."]
    for attr in &input.attrs {
        if attr.path().is_ident("table_name") {
            if let Meta::NameValue(meta) = &attr.meta {
                if let syn::Expr::Lit(expr_lit) = &meta.value {
                    if let Lit::Str(lit_str) = &expr_lit.lit {
                        return lit_str.value();
                    }
                }
            }
        }
    }

    to_snake_case(&input.ident.to_string())
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(ch.to_lowercase().next().unwrap());
    }
    result
}
