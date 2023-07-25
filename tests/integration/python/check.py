from pyspark.sql import SparkSession
import csv
import argparse
import unittest


def check(args):
    tc = unittest.TestCase()
    spark = SparkSession.builder.remote(args.sparkurl).getOrCreate()

    sql = "SELECT * FROM s1.t1 ORDER BY id ASC"
    print(f"Executing sql: {sql}")
    df = spark.sql(sql).collect()
    for row in df:
        print(row)

    with open(args.file, newline='') as insert_csv_file:
        csv_result = csv.reader(insert_csv_file)
        for (row1, row2) in zip(df, csv_result):
            print(f"Row1: {row1}, row 2: {row2}")
            tc.assertEqual(row1[0], int(row2[0]))
            tc.assertEqual(row1[1], row2[1])
            tc.assertEqual(row1[2], int(row2[2]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test icelake with spark')
    parser.add_argument('-s', dest='sparkurl', type=str, help='Spark remote url')
    parser.add_argument("-f", dest='file', type=str, help='Path to query csv file')

    check(parser.parse_args())
