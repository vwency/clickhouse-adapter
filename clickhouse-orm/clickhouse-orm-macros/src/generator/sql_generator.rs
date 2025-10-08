use crate::adapters::field_extractor::extract_fields;
use crate::adapters::head_options_macros::table_options::TableOptions;
use crate::generator::sql_builder::build_create_table_sql;
use syn::DeriveInput;

pub fn generate_create_table_sql(
    input: &DeriveInput,
    table_name: &str,
    options: &TableOptions,
) -> String {
    let fields = extract_fields(input);
    build_create_table_sql(table_name, &fields, options)
}
