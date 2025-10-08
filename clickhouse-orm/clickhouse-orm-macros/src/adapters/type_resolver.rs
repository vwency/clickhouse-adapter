use syn::PathSegment;

const OPTION_TYPE: &str = "Option";

pub fn map_segment_to_clickhouse(segment: &PathSegment) -> String {
    let type_str = segment.ident.to_string();

    if type_str == OPTION_TYPE {
        return map_option_type(segment);
    }

    super::mapping::map_primitive_type(&type_str)
}

fn map_option_type(segment: &PathSegment) -> String {
    match &segment.arguments {
        syn::PathArguments::AngleBracketed(args) => args
            .args
            .first()
            .and_then(|arg| match arg {
                syn::GenericArgument::Type(inner_ty) => Some(inner_ty),
                _ => None,
            })
            .map(|inner_ty| {
                let inner = super::mapping::map_rust_type_to_clickhouse(inner_ty);
                super::mapping::wrap_nullable(inner)
            })
            .unwrap_or_else(super::mapping::get_default_type),
        _ => super::mapping::get_default_type(),
    }
}
