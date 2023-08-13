use arrow::{
    array::{Int32Array, StringArray},
    datatypes::{DataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use deltalake::operations::DeltaOps;
use deltalake::{builder::DeltaTableBuilder, DeltaTable};
use std::collections::HashMap;
use std::sync::Arc;

fn get_data_to_write() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let ids: Vec<i32> = (1..=10).map(i32::from).collect();
    let names: Vec<String> = ids.iter().map(|x| format!("item {x}")).collect();

    let id_values = Int32Array::from(ids);
    let name_values = StringArray::from(names);

    RecordBatch::try_new(schema, vec![Arc::new(id_values), Arc::new(name_values)]).unwrap()
}

async fn append_to_table(
    path: String,
    backend_config: HashMap<String, String>,
    batch: RecordBatch,
) -> DeltaTable {

    let table = DeltaTableBuilder::from_uri(path)
        .with_storage_options(backend_config)
        .build()
        .unwrap();

    let ops = DeltaOps::from(table);

    let commit_result = ops.write(vec![batch.clone()]).await.unwrap();
    commit_result
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let s3_storage_location = "s3a://delta/table1".to_string();

    let mut backend_config: HashMap<String, String> = HashMap::new();
    //backend_config.insert("AWS_REGION".to_string(), region);
    backend_config.insert("AWS_ACCESS_KEY_ID".to_string(), "minioadmin".to_string());
    backend_config.insert("AWS_SECRET_ACCESS_KEY".to_string(), "minioadmin".to_string());
    backend_config.insert("AWS_ENDPOINT".to_string(), "http://minio:9000".to_string());
    backend_config.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());

    let batch = get_data_to_write();
    let table = append_to_table(
        s3_storage_location,
        backend_config,
        batch,
    )
    .await;
    println!("Data inserted with version : {}", table.version());
}