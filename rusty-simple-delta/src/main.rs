use std::sync::Arc;
use deltalake::{builder::DeltaTableBuilder, DeltaTable};
use deltalake::operations::DeltaOps;
use arrow::{
    array::{Int32Array, StringArray},
    datatypes::{DataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use deltalake::operations::create::CreateBuilder;
use deltalake::{SchemaDataType};


async fn create_table(path: &str) -> DeltaTable {
    let builder = CreateBuilder::new()
        .with_location(path)
        .with_column(
            "name",
            SchemaDataType::primitive(String::from("string")),
            false,
            Default::default(),
        )
        .with_column(
            "age",
            SchemaDataType::primitive(String::from("integer")),
            false,
            Default::default(),
        );

    builder.await.unwrap()
}

fn create_batch() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));

    let ages : Vec<i32> = (1..=100).map(i32::from).collect();
    let names : Vec<String> = ages.iter().map(|x| format!("username-{x}")).collect();

    let age_values = Int32Array::from(ages);
    let name_values = StringArray::from(names);

    RecordBatch::try_new(schema, vec![Arc::new(name_values), Arc::new(age_values)]).unwrap()
}

async fn append_to_table(path: String,batch: RecordBatch) -> DeltaTable {

    let table = DeltaTableBuilder::from_uri(path)
        .build()
        .unwrap();

    let ops = DeltaOps::from(table);

    let commit_result = ops.write(vec![batch.clone()]).await.unwrap();
    commit_result
}


#[tokio::main(flavor = "current_thread")]
async fn main() {
    let table = create_table("file:/my/given/path/data-engineering-rust-demo/rusty-simple-delta/data/simple-table").await;
    println!("Table created with version : {}", table.version());

    let batch = create_batch();
    let table = append_to_table("file:/my/given/path/data-engineering-rust-demo/rusty-simple-delta/data/simple-table".to_string(), batch).await;
    println!("Data inserted with version : {}", table.version());
    
}