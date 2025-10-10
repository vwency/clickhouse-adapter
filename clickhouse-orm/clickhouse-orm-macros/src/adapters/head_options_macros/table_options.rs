use crate::domain::table_options::TableOptions;
use syn::{DeriveInput, Lit};

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
            if attr.path().is_ident("clickhouse") {
                // Используем parse_nested_meta для корректного парсинга
                let _ = attr.parse_nested_meta(|meta| {
                    let path = meta.path.get_ident().map(|i| i.to_string());

                    // Получаем значение после знака =
                    if let Ok(value) = meta.value() {
                        if let Ok(lit) = value.parse::<Lit>() {
                            if let Lit::Str(lit_str) = lit {
                                let val = lit_str.value();

                                match path.as_deref() {
                                    Some("engine") => options.engine = Some(val),
                                    Some("order_by") => options.order_by = Some(val),
                                    Some("partition_by") => options.partition_by = Some(val),
                                    Some("primary_key") => options.primary_key = Some(val),
                                    Some("sample_by") => options.sample_by = Some(val),
                                    Some("settings") => options.settings = Some(val),
                                    _ => {}
                                }
                            }
                        }
                    }

                    Ok(())
                });
            }
        }

        options
    }
}
