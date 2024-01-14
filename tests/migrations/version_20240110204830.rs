use creed::schema::Schema;
use creed::migrate::Executor;

fn pre_up(from_schema: &Schema) -> creed::Result<Schema> {
    Ok(from_schema.clone())
}

fn up(executor: &mut Executor, schema: &Schema) -> creed::Result<()> {
    Ok(())
}

fn down(executor: &mut Executor, schema: &Schema) -> creed::Result<()> {
    Ok(())
}

fn description() -> &'static str {
    "test migration #1"
}
