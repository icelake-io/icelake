from pyspark.sql import SparkSession
import csv
import argparse


def check(args):
    spark = SparkSession.builder.remote(args.sparkurl).getOrCreate()

    init_table_sqls = [
        "CREATE SCHEMA IF NOT EXISTS s1",
        "DROP TABLE IF EXISTS s1.t1",
        """
        CREATE TABLE s1.t1
        (
          id bigint,
          name string,
          distance bigint
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2');
        """
    ]

    for sql in init_table_sqls:
        print(f"Executing sql: {sql}")
        spark.sql(sql)

    with open(args.file, newline='') as insert_csv_file:
        inserts = ", ".join([f"""({row[0]}, "{row[1]}", {row[2]})""" for row in csv.reader(insert_csv_file)])
        sql = f"""
         INSERT INTO s1.t1 VALUES {inserts}
         """
        print(f"Executing sql: {sql}")
        spark.sql(sql)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test icelake with spark')
    parser.add_argument('-s', dest='sparkurl', type=str, help='Spark remote url')
    parser.add_argument("-f", dest='file', type=str, help='Path to insert csv file')

    check(parser.parse_args())
