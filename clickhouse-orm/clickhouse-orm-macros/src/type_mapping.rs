use syn::{Field, Meta};

pub fn extract_clickhouse_type(field: &Field) -> String {
    // Ищем атрибут #[clickhouse(type = "...")]
    for attr in &field.attrs {
        if attr.path().is_ident("clickhouse") {
            if let Meta::List(meta_list) = &attr.meta {
                if let Ok(nested) = meta_list.parse_args::<syn::MetaNameValue>() {
                    if nested.path.is_ident("type") {
                        if let syn::Expr::Lit(expr_lit) = &nested.value {
                            if let syn::Lit::Str(lit_str) = &expr_lit.lit {
                                return lit_str.value();
                            }
                        }
                    }
                }
            }
        }
    }

    // Fallback: маппинг по типу Rust
    map_rust_type_to_clickhouse(&field.ty)
}

pub fn map_rust_type_to_clickhouse(ty: &syn::Type) -> String {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            let type_str = segment.ident.to_string();

            // Проверяем Option<T> первым
            if type_str == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                        let inner = map_rust_type_to_clickhouse(inner_ty);
                        return format!("Nullable({})", inner);
                    }
                }
            }

            // Базовые типы
            return match type_str.as_str() {
                "u8" => "UInt8",
                "u16" => "UInt16",
                "u32" => "UInt32",
                "u64" => "UInt64",
                "i8" => "Int8",
                "i16" => "Int16",
                "i32" => "Int32",
                "i64" => "Int64",
                "f32" => "Float32",
                "f64" => "Float64",
                "bool" => "UInt8",
                "String" => "String",
                "DateTime" => "DateTime('UTC')",
                "Date" => "Date",
                "Uuid" => "UUID",
                _ => "String", // fallback
            }
            .to_string();
        }
    }
    "String".to_string()
}
