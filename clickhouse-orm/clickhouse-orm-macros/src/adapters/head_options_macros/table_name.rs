use syn::{DeriveInput, Lit, Meta};

pub fn get_table_name(input: &DeriveInput) -> String {
    input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("table_name"))
        .and_then(extract_table_name_value)
        .unwrap_or_else(|| to_snake_case(&input.ident.to_string()))
}

fn extract_table_name_value(attr: &syn::Attribute) -> Option<String> {
    let Meta::NameValue(meta_name_value) = &attr.meta else {
        return None;
    };

    let syn::Expr::Lit(expr_lit) = &meta_name_value.value else {
        return None;
    };

    let Lit::Str(lit_str) = &expr_lit.lit else {
        return None;
    };

    Some(lit_str.value())
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::new();

    for c in s.chars() {
        if c.is_uppercase() {
            if !result.is_empty() {
                result.push('_');
            }
            result.extend(c.to_lowercase());
        } else {
            result.push(c);
        }
    }

    result
}
