from pyspark.sql import SparkSession
import argparse


def check(args):
    spark = SparkSession.builder.remote(args.sparkurl).getOrCreate()

    init_table_sqls = [
        "CREATE SCHEMA IF NOT EXISTS s1",
        "DROP TABLE IF EXISTS s1.t1",
        """
        CREATE TABLE s1.t1
        (
          id long,
          v_int int,
          v_long long,
          v_float float,
          v_double double,
          v_varchar string,
          v_bool boolean,
          v_date date,
          v_timestamp timestamp,
          v_decimal decimal(36, 10),
          v_ts_ntz timestamp_ntz
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2');
        """
    ]

    for sql in init_table_sqls:
        print(f"Executing sql: {sql}")
        spark.sql(sql)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test icelake with spark')
    parser.add_argument('-s', dest='sparkurl', type=str, help='Spark remote url')
    parser.add_argument("-f", dest='file', type=str, help='Path to insert csv file')

    check(parser.parse_args())
