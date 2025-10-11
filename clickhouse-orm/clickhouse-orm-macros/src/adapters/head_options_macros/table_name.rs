use syn::{DeriveInput, Lit, Meta};

pub fn get_table_name(input: &DeriveInput) -> String {
    for attr in &input.attrs {
        if attr.path().is_ident("table_name") {
            if let Meta::NameValue(meta_name_value) = &attr.meta {
                if let syn::Expr::Lit(expr_lit) = &meta_name_value.value {
                    if let Lit::Str(lit_str) = &expr_lit.lit {
                        return lit_str.value();
                    }
                }
            }
        }
    }

    to_snake_case(&input.ident.to_string())
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c.is_uppercase() {
            if !result.is_empty() {
                result.push('_');
            }
            result.push(c.to_lowercase().next().unwrap());
        } else {
            result.push(c);
        }
    }

    result
}
