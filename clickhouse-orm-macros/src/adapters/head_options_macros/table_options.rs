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
            if attr.path().is_ident("ch_config") {
                Self::process_ch_config_attribute(&mut options, attr);
            }
        }

        options
    }

    fn process_ch_config_attribute(options: &mut Self, attr: &syn::Attribute) {
        let Meta::List(meta_list) = &attr.meta else {
            return;
        };

        let Ok(nested_metas) = meta_list.parse_args_with(
            syn::punctuated::Punctuated::<MetaNameValue, syn::Token![,]>::parse_terminated,
        ) else {
            return;
        };

        for meta in nested_metas {
            Self::apply_meta_value(options, meta);
        }
    }

    fn apply_meta_value(options: &mut Self, meta: MetaNameValue) {
        let Some(key) = meta.path.get_ident().map(|i| i.to_string()) else {
            return;
        };

        let syn::Expr::Lit(expr_lit) = meta.value else {
            return;
        };

        let Lit::Str(lit_str) = expr_lit.lit else {
            return;
        };

        let value = lit_str.value();

        match key.as_str() {
            "engine" => options.engine = Some(value),
            "order_by" => options.order_by = Some(value),
            "partition_by" => options.partition_by = Some(value),
            "primary_key" => options.primary_key = Some(value),
            "sample_by" => options.sample_by = Some(value),
            "settings" => options.settings = Some(value),
            _ => {}
        }
    }
}
