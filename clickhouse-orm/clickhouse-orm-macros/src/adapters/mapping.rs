use once_cell::sync::Lazy;
use std::collections::HashMap;
use syn::Field;

const DEFAULT_TYPE: &str = "String";
const NULLABLE_TEMPLATE: &str = "Nullable({})";
const DATETIME_UTC: &str = "DateTime('UTC')";

static TYPE_MAP: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
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
});

pub fn extract_clickhouse_type(field: &Field) -> String {
    super::attribute_parser::find_clickhouse_type_attr(field)
        .unwrap_or_else(|| map_rust_type_to_clickhouse(&field.ty))
}

pub fn map_rust_type_to_clickhouse(ty: &syn::Type) -> String {
    match ty {
        syn::Type::Path(type_path) => type_path
            .path
            .segments
            .last()
            .map(super::type_resolver::map_segment_to_clickhouse)
            .unwrap_or_else(get_default_type),
        _ => get_default_type(),
    }
}

pub fn map_primitive_type(type_str: &str) -> String {
    TYPE_MAP.get(type_str).unwrap_or(&DEFAULT_TYPE).to_string()
}

pub fn wrap_nullable(inner_type: String) -> String {
    NULLABLE_TEMPLATE.replace("{}", &inner_type)
}

pub fn get_default_type() -> String {
    DEFAULT_TYPE.to_string()
}
