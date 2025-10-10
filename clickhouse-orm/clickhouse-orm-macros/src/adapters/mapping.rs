use crate::domain::types::TYPE_MAP;
use crate::domain::types::{DEFAULT_TYPE, NULLABLE_TEMPLATE};
use syn::Field;

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
