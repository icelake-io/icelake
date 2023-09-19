
import argparse
import unittest
from pyspark.sql import SparkSession


def execute(args):
    spark = SparkSession.builder.remote(args.sparkurl).getOrCreate()

    sql = args.sql
    print(f"Executing sql: {sql}")
    df1 = spark.sql(sql).collect()
    print(f"Len of df1: {len(df1)}")
    print(f"{df1}")

    # diff_df = df1.exceptAll(df2).collect()
    # tc.assertEqual(len(diff_df), 0)

    # diff_df = df2.exceptAll(df1).collect()
    # tc.assertEqual(len(diff_df), 0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test icelake with spark")
    parser.add_argument("-s", dest="sparkurl", type=str, help="Spark remote url")
    parser.add_argument("-q", dest="sql", type=str, help="execute sql")
    execute(parser.parse_args())
