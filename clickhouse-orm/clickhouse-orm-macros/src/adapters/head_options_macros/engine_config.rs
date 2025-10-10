use crate::domain::engine_config::EngineConfig;
use syn::{Attribute, Lit};

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

            // Используем parse_nested_meta для правильного парсинга
            let _ = attr.parse_nested_meta(|meta| {
                let key = meta.path.get_ident().map(|i| i.to_string());

                if let Ok(value_parser) = meta.value() {
                    if let Ok(lit) = value_parser.parse::<Lit>() {
                        if let Lit::Str(lit_str) = lit {
                            let value = lit_str.value();

                            match key.as_deref() {
                                Some("engine") => {
                                    config.engine_type = value;
                                }
                                Some("zk_path") => {
                                    config.zk_path = Some(value);
                                }
                                Some("replica") => {
                                    config.replica = Some(value);
                                }
                                Some("sign_column") => {
                                    config.sign_column = Some(value);
                                }
                                Some("version_column") => {
                                    config.version_column = Some(value);
                                }
                                Some("columns") => {
                                    // Парсим список колонок, разделенных запятыми
                                    config.columns = Some(
                                        value.split(',').map(|s| s.trim().to_string()).collect(),
                                    );
                                }
                                _ => {}
                            }
                        }
                    }
                }

                Ok(())
            });
        }

        config
    }
}
