from pyspark.sql import SparkSession
import argparse


def check(args):
    spark = SparkSession.builder.remote(args.sparkurl).getOrCreate()

    for sql in args.sql:
        print(f"Executing sql: {sql}")
        spark.sql(sql)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test icelake with spark')
    parser.add_argument('-s', dest='sparkurl', type=str, help='Spark remote url')
    parser.add_argument('--sql', dest='sql', nargs = '+',type=str, help='SQL to execute')

    check(parser.parse_args())
