use crate::domain::engine_config::EngineConfig;
use crate::domain::field_definition::FieldDefinition;
use crate::domain::table_options::TableOptions;

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
    engine_config: &EngineConfig,
) -> String {
    let fields_str = fields.iter().map(|f| f.to_sql()).collect::<Vec<_>>().join(FIELD_SEPARATOR);

    let mut sql = format!("{} {} ({})", CREATE_TABLE_TEMPLATE, table_name, fields_str);

    // Добавляем ENGINE из engine_config
    let engine_clause = generate_engine_clause(engine_config);
    sql.push_str(ENGINE_PREFIX);
    sql.push_str(&engine_clause);

    // Добавляем PRIMARY KEY если указан
    append_clause(&mut sql, PRIMARY_KEY_PREFIX, options.primary_key.as_deref());

    // Добавляем ORDER BY
    append_clause(&mut sql, ORDER_BY_PREFIX, options.order_by.as_deref());

    // Добавляем PARTITION BY если указан
    append_clause(&mut sql, PARTITION_BY_PREFIX, options.partition_by.as_deref());

    // Добавляем SAMPLE BY если указан
    append_clause(&mut sql, SAMPLE_BY_PREFIX, options.sample_by.as_deref());

    // Добавляем SETTINGS если указаны
    append_clause(&mut sql, SETTINGS_PREFIX, options.settings.as_deref());

    sql
}

fn generate_engine_clause(config: &EngineConfig) -> String {
    match config.engine_type.as_str() {
        "ReplicatedMergeTree" => {
            let zk_path = config.zk_path.as_deref().unwrap_or("/clickhouse/tables/{shard}/default");
            let replica = config.replica.as_deref().unwrap_or("{replica}");
            format!("ReplicatedMergeTree('{}', '{}')", zk_path, replica)
        }
        "CollapsingMergeTree" => {
            let sign = config.sign_column.as_deref().unwrap_or("sign");
            format!("CollapsingMergeTree({})", sign)
        }
        "VersionedCollapsingMergeTree" => {
            let sign = config.sign_column.as_deref().unwrap_or("sign");
            let version = config.version_column.as_deref().unwrap_or("version");
            format!("VersionedCollapsingMergeTree({}, {})", sign, version)
        }
        "SummingMergeTree" => {
            if let Some(columns) = &config.columns {
                let cols = columns.join(", ");
                format!("SummingMergeTree(({}))", cols)
            } else {
                "SummingMergeTree()".to_string()
            }
        }
        "AggregatingMergeTree" => "AggregatingMergeTree()".to_string(),
        "ReplacingMergeTree" => {
            if let Some(version_col) = &config.version_column {
                format!("ReplacingMergeTree({})", version_col)
            } else {
                "ReplacingMergeTree()".to_string()
            }
        }
        "GraphiteMergeTree" => "GraphiteMergeTree('graphite_rollup')".to_string(),
        _ => "MergeTree()".to_string(),
    }
}

fn append_clause(sql: &mut String, prefix: &str, value: Option<&str>) {
    if let Some(val) = value {
        sql.push_str(prefix);
        sql.push_str(val);
    }
}
