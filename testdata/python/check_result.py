import argparse
import unittest
from pyspark.sql import SparkSession


def check(args):
    tc = unittest.TestCase()
    spark = SparkSession.builder.remote(args.sparkurl).getOrCreate()

    sql = args.sql
    print(f"Executing sql: {sql}")
    df = spark.sql(sql).collect()
    print(f"Len of df: {len(df)}")
    df_result = df[0][0]
    num = args.num

    tc.assertEqual(num, num)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test icelake with spark")
    parser.add_argument("-s", dest="sparkurl", type=str, help="Spark remote url")
    parser.add_argument("-q", dest="sql", type=str, help="query sql")
    parser.add_argument("-n", dest="num", type=int, help="result number")
    check(parser.parse_args())
