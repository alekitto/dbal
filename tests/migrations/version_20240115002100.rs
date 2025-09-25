use creed::migrate::Executor;
use creed::r#type::{DATETIMETZ, STRING, TEXT};
use creed::Result;
use creed::schema::{Column, Schema, Table};

pub fn description() -> &'static str {
    "create client_credentials table"
}

pub fn pre_up(schema: &Schema) -> Result<Schema> {
    let mut new_schema = schema.clone();
    let mut table = new_schema.create_table("client_credential")?;
    table.add_column(Column::builder("client_id", STRING)?.set_notnull(true));
    table.add_column(Column::builder("secret", TEXT)?.set_notnull(true));
    table.add_column(Column::builder("expires_at", DATETIMETZ)?.set_notnull(true));
    table.add_column(Column::builder("created_at", DATETIMETZ)?.set_notnull(true));

    table.set_primary_key(&["client_id"], None)?;

    Ok(new_schema)
}

pub fn up(_: &mut Executor, schema: &Schema) -> Result<()> {
    Ok(())
}

pub fn pre_down(schema: &Schema) -> Result<Schema> {
    let mut new_schema = schema.clone();
    new_schema.drop_table("client_credential");

    Ok(new_schema)
}

pub fn down(_: &mut Executor, schema: &Schema) -> Result<()> {
    Ok(())
}