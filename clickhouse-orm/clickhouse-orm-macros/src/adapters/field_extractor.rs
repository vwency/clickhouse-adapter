use crate::adapters::mapping::extract_clickhouse_type;
use syn::{Data, DeriveInput, Fields};

const DEFAULT_FIELD_NAME: &str = "id";
const DEFAULT_FIELD_TYPE: &str = "UInt64";

pub fn extract_fields(input: &DeriveInput) -> Vec<FieldDefinition> {
    let mut fields = Vec::new();

    if let Data::Struct(data) = &input.data {
        if let Fields::Named(named) = &data.fields {
            for field in &named.named {
                let field_name = field.ident.as_ref().unwrap().to_string();
                let field_type = extract_clickhouse_type(field);
                fields.push(FieldDefinition { name: field_name, field_type });
            }
        }
    }

    if fields.is_empty() {
        fields.push(FieldDefinition {
            name: DEFAULT_FIELD_NAME.to_string(),
            field_type: DEFAULT_FIELD_TYPE.to_string(),
        });
    }

    fields
}

pub struct FieldDefinition {
    pub name: String,
    pub field_type: String,
}

impl FieldDefinition {
    pub fn to_sql(&self) -> String {
        format!("{} {}", self.name, self.field_type)
    }
}
