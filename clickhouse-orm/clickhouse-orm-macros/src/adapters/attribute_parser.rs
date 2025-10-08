use syn::{Field, Meta};

const CLICKHOUSE_ATTR: &str = "clickhouse";
const TYPE_ATTR: &str = "type";

pub fn find_clickhouse_type_attr(field: &Field) -> Option<String> {
    field
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident(CLICKHOUSE_ATTR))
        .and_then(|attr| parse_type_meta(&attr.meta))
}

fn parse_type_meta(meta: &Meta) -> Option<String> {
    match meta {
        Meta::List(meta_list) => meta_list
            .parse_args::<syn::MetaNameValue>()
            .ok()
            .filter(|nested| nested.path.is_ident(TYPE_ATTR))
            .and_then(|nested| extract_string_literal(&nested.value)),
        _ => None,
    }
}

fn extract_string_literal(expr: &syn::Expr) -> Option<String> {
    match expr {
        syn::Expr::Lit(expr_lit) => match &expr_lit.lit {
            syn::Lit::Str(lit_str) => Some(lit_str.value()),
            _ => None,
        },
        _ => None,
    }
}
