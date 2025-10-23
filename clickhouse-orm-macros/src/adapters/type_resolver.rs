use crate::domain::types::OPTION_TYPE;
use syn::{GenericArgument, PathArguments, PathSegment};

pub fn map_segment_to_clickhouse(segment: &PathSegment) -> String {
    let type_str = segment.ident.to_string();

    if type_str == OPTION_TYPE {
        map_option_type(segment)
    } else {
        super::mapping::map_primitive_type(&type_str)
    }
}

fn map_option_type(segment: &PathSegment) -> String {
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return super::mapping::get_default_type();
    };

    args.args
        .first()
        .and_then(extract_inner_type)
        .map(map_to_nullable_clickhouse_type)
        .unwrap_or_else(super::mapping::get_default_type)
}

fn extract_inner_type(arg: &GenericArgument) -> Option<&syn::Type> {
    match arg {
        GenericArgument::Type(inner_ty) => Some(inner_ty),
        _ => None,
    }
}

fn map_to_nullable_clickhouse_type(inner_ty: &syn::Type) -> String {
    let inner = super::mapping::map_rust_type_to_clickhouse(inner_ty);
    super::mapping::wrap_nullable(inner)
}
