use crate::domain::engine_config::EngineConfig;
use syn::{Attribute, Meta};

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
            if !attr.path().is_ident("clickhouse") {
                continue;
            }

            if let Meta::List(ref meta_list) = attr.meta {
                for nested in meta_list.tokens.clone() {
                    let nested_str = nested.to_string();

                    if nested_str.starts_with("engine") {
                        config.engine_type = Self::extract_string_value(&nested_str, "engine");
                    } else if nested_str.starts_with("zk_path") {
                        config.zk_path = Some(Self::extract_string_value(&nested_str, "zk_path"));
                    } else if nested_str.starts_with("replica") {
                        config.replica = Some(Self::extract_string_value(&nested_str, "replica"));
                    } else if nested_str.starts_with("sign_column") {
                        config.sign_column =
                            Some(Self::extract_string_value(&nested_str, "sign_column"));
                    } else if nested_str.starts_with("version_column") {
                        config.version_column =
                            Some(Self::extract_string_value(&nested_str, "version_column"));
                    }
                }
            }
        }

        config
    }

    fn extract_string_value(token_str: &str, key: &str) -> String {
        token_str
            .trim_start_matches(key)
            .trim_start_matches('=')
            .trim_matches('"')
            .trim()
            .to_string()
    }
}
