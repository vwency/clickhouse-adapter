use crate::domain::types::{CLICKHOUSE_ATTR, TYPE_ATTR};
use syn::{Expr, Field, Lit, Meta};

pub fn find_clickhouse_type_attr(field: &Field) -> Option<String> {
    field
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident(CLICKHOUSE_ATTR))
        .and_then(|attr| parse_type_meta(&attr.meta))
}

fn parse_type_meta(meta: &Meta) -> Option<String> {
    let Meta::List(meta_list) = meta else {
        return None;
    };

    meta_list
        .parse_args::<syn::MetaNameValue>()
        .ok()
        .filter(|nested| nested.path.is_ident(TYPE_ATTR))
        .and_then(|nested| extract_string_literal(&nested.value))
}

fn extract_string_literal(expr: &Expr) -> Option<String> {
    let Expr::Lit(expr_lit) = expr else {
        return None;
    };

    let Lit::Str(lit_str) = &expr_lit.lit else {
        return None;
    };

    Some(lit_str.value())
}
