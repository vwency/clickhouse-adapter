use crate::domain::table_options::TableOptions;
use syn::{DeriveInput, Lit, Meta, MetaNameValue};

impl Default for TableOptions {
    fn default() -> Self {
        Self {
            engine: Some("MergeTree".to_string()),
            order_by: Some("id".to_string()),
            partition_by: None,
            primary_key: None,
            sample_by: None,
            settings: None,
        }
    }
}

impl TableOptions {
    pub fn from_derive_input(input: &DeriveInput) -> Self {
        let mut options = Self::default();

        for attr in &input.attrs {
            // Проверяем атрибут ch_config
            if attr.path().is_ident("ch_config") {
                if let Meta::List(meta_list) = &attr.meta {
                    // Парсим все вложенные атрибуты как список MetaNameValue
                    let parsed = meta_list.parse_args_with(
                        syn::punctuated::Punctuated::<MetaNameValue, syn::Token![,]>::parse_terminated
                    );

                    if let Ok(nested_metas) = parsed {
                        for meta in nested_metas {
                            let key = meta.path.get_ident().map(|i| i.to_string());

                            if let syn::Expr::Lit(expr_lit) = &meta.value {
                                if let Lit::Str(lit_str) = &expr_lit.lit {
                                    let value = lit_str.value();

                                    match key.as_deref() {
                                        Some("engine") => options.engine = Some(value),
                                        Some("order_by") => options.order_by = Some(value),
                                        Some("partition_by") => options.partition_by = Some(value),
                                        Some("primary_key") => options.primary_key = Some(value),
                                        Some("sample_by") => options.sample_by = Some(value),
                                        Some("settings") => options.settings = Some(value),
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        options
    }
}
