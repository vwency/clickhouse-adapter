use crate::domain::engine_config::EngineConfig;
use syn::{Attribute, Lit, Meta};

impl EngineConfig {
    pub fn from_attributes(attrs: &[Attribute]) -> Self {
        let mut config = EngineConfig {
            engine_type: "MergeTree".to_string(),
            zk_path: None,
            replica: None,
            sign_column: None,
            version_column: None,
            columns: None,
        };

        for attr in attrs {
            if attr.path().is_ident("table_engine") {
                if let Meta::NameValue(ref nv) = attr.meta {
                    if let syn::Expr::Lit(ref expr_lit) = nv.value {
                        if let Lit::Str(ref lit_str) = expr_lit.lit {
                            config.engine_type = lit_str.value();
                        }
                    }
                }
            }
        }

        for attr in attrs {
            if attr.path().is_ident("table_engine_options") {
                if let Meta::List(ref meta_list) = attr.meta {
                    let tokens_str = meta_list.tokens.to_string();

                    for pair in tokens_str.split(',') {
                        let pair = pair.trim();

                        if let Some((key, value)) = pair.split_once('=') {
                            let key = key.trim();
                            let value =
                                value.trim().trim_start_matches('"').trim_end_matches('"').trim();

                            match key {
                                "zk_path" => config.zk_path = Some(value.to_string()),
                                "replica" => config.replica = Some(value.to_string()),
                                "sign_column" => config.sign_column = Some(value.to_string()),
                                "version_column" => config.version_column = Some(value.to_string()),
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        config
    }
}
