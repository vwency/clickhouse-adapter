use crate::table_options::TableOptions;
use syn::{Data, DeriveInput, Fields};

pub fn generate_create_table_sql(
    input: &DeriveInput,
    table_name: &str,
    options: &TableOptions,
) -> String {
    let mut fields = Vec::new();

    if let Data::Struct(data) = &input.data {
        if let Fields::Named(named) = &data.fields {
            for field in &named.named {
                let field_name = field.ident.as_ref().unwrap().to_string();
                let field_type = crate::type_mapping::extract_clickhouse_type(field);
                fields.push(format!("{} {}", field_name, field_type));
            }
        }
    }

    if fields.is_empty() {
        fields.push("id UInt64".to_string());
    }

    let fields_str = fields.join(", ");
    let mut sql = format!("CREATE TABLE IF NOT EXISTS {} ({})", table_name, fields_str);

    // ENGINE
    if let Some(engine) = &options.engine {
        sql.push_str(&format!(" ENGINE = {}", engine));
    }

    // PRIMARY KEY (опционально, если отличается от ORDER BY)
    if let Some(primary_key) = &options.primary_key {
        sql.push_str(&format!(" PRIMARY KEY {}", primary_key));
    }

    // ORDER BY
    if let Some(order_by) = &options.order_by {
        sql.push_str(&format!(" ORDER BY {}", order_by));
    }

    // PARTITION BY
    if let Some(partition_by) = &options.partition_by {
        sql.push_str(&format!(" PARTITION BY {}", partition_by));
    }

    // SAMPLE BY
    if let Some(sample_by) = &options.sample_by {
        sql.push_str(&format!(" SAMPLE BY {}", sample_by));
    }

    // SETTINGS
    if let Some(settings) = &options.settings {
        sql.push_str(&format!(" SETTINGS {}", settings));
    }

    sql
}
