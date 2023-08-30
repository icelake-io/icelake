use std::{io::Read, sync::Arc};

use arrow_array::RecordBatch;
use arrow_csv::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use serde::Deserialize;

/// TestCase include init sqls, write data and query sqls.
/// These are generated from toml file.
/// For a new test, user just need to add a new toml file.
#[derive(Debug)]
pub struct TestCase {
    pub init_sqls: Vec<String>,
    pub write_date: Vec<RecordBatch>,
    pub query_sql: Vec<[String; 2]>,
    pub table_root: String,
    pub table_name: String,
}

#[derive(Deserialize, Debug)]
struct TomlConent {
    pub schema_name: String,
    pub table_name: String,
    pub create_table_sql: String,
    pub table_schema: Vec<String>,
    pub data: String,
    pub query: Vec<String>,
}

impl TestCase {
    /// Parse toml file and generate a TestCase.
    pub fn parse<R: Read>(mut buf: R) -> Self {
        let mut content = String::new();
        buf.read_to_string(&mut content).unwrap();
        let toml_content: TomlConent = toml::from_str(&content).unwrap();

        // Parse write data
        let schema = Arc::new(Self::parse_schema(toml_content.table_schema));

        let csv_reader = ReaderBuilder::new(schema.clone())
            .has_header(false)
            .build(toml_content.data.as_bytes())
            .unwrap();
        let write_data = csv_reader.map(|r| r.unwrap()).collect::<Vec<RecordBatch>>();

        // Generate init sqls
        let mut init_sqls = Vec::with_capacity(5);
        let tmp_table_name = "tmp";
        init_sqls.push(format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            toml_content.schema_name
        ));
        init_sqls.push(format!(
            "DROP TABLE IF EXISTS {}.{}",
            toml_content.schema_name, tmp_table_name
        ));
        init_sqls.push(
            toml_content
                .create_table_sql
                .replace(&toml_content.table_name, tmp_table_name),
        );
        init_sqls.push(format!(
            "DROP TABLE IF EXISTS {}.{}",
            toml_content.schema_name, toml_content.table_name
        ));
        init_sqls.push(toml_content.create_table_sql);

        // Generate insert sqls
        init_sqls.extend(Self::generate_insert_sql(
            &schema,
            toml_content.data,
            &toml_content.schema_name,
            tmp_table_name,
        ));

        let mut query_sql = Vec::with_capacity(toml_content.query.len());
        for sql in toml_content.query {
            let tmp_sql = sql.replace(&toml_content.table_name, tmp_table_name);
            query_sql.push([sql, tmp_sql]);
        }

        Self {
            init_sqls,
            write_date: write_data,
            query_sql,
            table_root: format!(
                "demo/{}/{}",
                toml_content.schema_name, toml_content.table_name
            ),
            table_name: toml_content.table_name,
        }
    }

    fn generate_insert_sql(
        schema: &Schema,
        data: String,
        schema_name: &str,
        tmp_table_name: &str,
    ) -> Vec<String> {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(data.as_bytes());
        rdr.records()
            .map(|result| {
                let record = result.unwrap();
                let items = record
                    .iter()
                    .enumerate()
                    .map(|(idx, s)| match schema.fields[idx].data_type() {
                        DataType::Timestamp(_, Some(_)) => format!("cast('{}' as timestamp)", s),
                        DataType::Timestamp(_, None) => format!("cast('{}' as timestamp_ntz)", s),
                        DataType::Date32 => format!("cast('{}' as date)", s),
                        DataType::Utf8 => format!("'{}'", s),
                        _ => s.to_string(),
                    })
                    .collect::<Vec<String>>();
                format!(
                    "INSERT INTO {}.{} VALUES ({})",
                    schema_name,
                    tmp_table_name,
                    items.join(",")
                )
            })
            .collect::<Vec<String>>()
    }

    fn parse_schema(schema: Vec<String>) -> Schema {
        Schema::new(
            schema
                .iter()
                .enumerate()
                .map(|(id, field)| {
                    let name = id.to_string();
                    match field.as_str() {
                        "long" => Field::new(name, DataType::Int64, false),
                        "int" => Field::new(name, DataType::Int32, true),
                        "float" => Field::new(name, DataType::Float32, true),
                        "double" => Field::new(name, DataType::Float64, true),
                        "string" => Field::new(name, DataType::Utf8, true),
                        "boolean" => Field::new(name, DataType::Boolean, true),
                        "date" => Field::new(name, DataType::Date32, true),
                        "decimal" => Field::new(name, DataType::Decimal128(36, 10), true),
                        str => {
                            if str.starts_with("timestamp") {
                                let res = str.split("with").collect::<Vec<&str>>();
                                assert!(res.len() == 1 || res.len() == 2);
                                if res.len() == 2 {
                                    Field::new(
                                        name,
                                        DataType::Timestamp(
                                            TimeUnit::Microsecond,
                                            Some(res[1].trim_start().into()),
                                        ),
                                        true,
                                    )
                                } else {
                                    Field::new(
                                        name,
                                        DataType::Timestamp(TimeUnit::Microsecond, None),
                                        true,
                                    )
                                }
                            } else {
                                unreachable!("Incorrect type: {}", str);
                            }
                        }
                    }
                })
                .collect::<Vec<Field>>(),
        )
    }
}

// #[cfg(test)]
// mod test {
//     #[test]
//     fn test() {
//         let file_name = format!("{}/../testdata/toml/test.toml", env!("CARGO_MANIFEST_DIR"),);
//         let file = std::fs::File::open(file_name).unwrap();
//         let test_case = super::TestCase::parse(file);
//         println!("{:#?}", test_case);
//         //
//         todo!();
//     }
// }
