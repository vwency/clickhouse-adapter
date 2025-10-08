use crate::adapters::field_extractor::FieldDefinition;
use crate::adapters::head_options_macros::table_options::TableOptions;

const CREATE_TABLE_TEMPLATE: &str = "CREATE TABLE IF NOT EXISTS";
const ENGINE_PREFIX: &str = " ENGINE = ";
const PRIMARY_KEY_PREFIX: &str = " PRIMARY KEY ";
const ORDER_BY_PREFIX: &str = " ORDER BY ";
const PARTITION_BY_PREFIX: &str = " PARTITION BY ";
const SAMPLE_BY_PREFIX: &str = " SAMPLE BY ";
const SETTINGS_PREFIX: &str = " SETTINGS ";
const FIELD_SEPARATOR: &str = ", ";

pub fn build_create_table_sql(
    table_name: &str,
    fields: &[FieldDefinition],
    options: &TableOptions,
) -> String {
    let fields_str = fields.iter().map(|f| f.to_sql()).collect::<Vec<_>>().join(FIELD_SEPARATOR);

    let mut sql = format!("{} {} ({})", CREATE_TABLE_TEMPLATE, table_name, fields_str);

    append_clause(&mut sql, ENGINE_PREFIX, options.engine.as_deref());
    append_clause(&mut sql, PRIMARY_KEY_PREFIX, options.primary_key.as_deref());
    append_clause(&mut sql, ORDER_BY_PREFIX, options.order_by.as_deref());
    append_clause(&mut sql, PARTITION_BY_PREFIX, options.partition_by.as_deref());
    append_clause(&mut sql, SAMPLE_BY_PREFIX, options.sample_by.as_deref());
    append_clause(&mut sql, SETTINGS_PREFIX, options.settings.as_deref());

    sql
}

fn append_clause(sql: &mut String, prefix: &str, value: Option<&str>) {
    if let Some(val) = value {
        sql.push_str(prefix);
        sql.push_str(val);
    }
}
