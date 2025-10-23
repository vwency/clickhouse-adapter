use once_cell::sync::Lazy;
use std::collections::HashMap;

pub const DEFAULT_TYPE: &str = "String";
pub const NULLABLE_TEMPLATE: &str = "Nullable({})";
pub const DATETIME_UTC: &str = "DateTime('UTC')";

pub const DEFAULT_FIELD_NAME: &str = "id";
pub const DEFAULT_FIELD_TYPE: &str = "UInt64";

pub const CLICKHOUSE_ATTR: &str = "clickhouse";
pub const TYPE_ATTR: &str = "type";

pub const OPTION_TYPE: &str = "Option";

pub fn type_map() -> HashMap<&'static str, &'static str> {
    let mut map = HashMap::new();
    map.insert("u8", "UInt8");
    map.insert("u16", "UInt16");
    map.insert("u32", "UInt32");
    map.insert("u64", "UInt64");
    map.insert("i8", "Int8");
    map.insert("i16", "Int16");
    map.insert("i32", "Int32");
    map.insert("i64", "Int64");
    map.insert("f32", "Float32");
    map.insert("f64", "Float64");
    map.insert("bool", "UInt8");
    map.insert("String", "String");
    map.insert("DateTime", DATETIME_UTC);
    map.insert("Date", "Date");
    map.insert("Uuid", "UUID");
    map
}

pub static TYPE_MAP: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(type_map);
