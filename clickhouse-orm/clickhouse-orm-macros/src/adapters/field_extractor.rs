use crate::adapters::mapping::extract_clickhouse_type;
use crate::domain::field_definition::FieldDefinition;
use crate::domain::types::{DEFAULT_FIELD_NAME, DEFAULT_FIELD_TYPE};
use syn::{Data, DeriveInput, Fields};
use Data::Struct;

pub fn extract_fields(input: &DeriveInput) -> Vec<FieldDefinition> {
    let mut fields = Vec::new();

    if let Struct(data) = &input.data {
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

impl FieldDefinition {
    pub fn to_sql(&self) -> String {
        format!("{} {}", self.name, self.field_type)
    }
}
