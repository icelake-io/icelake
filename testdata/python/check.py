import argparse
import csv
import unittest
from pyspark.sql import SparkSession
from datetime import date
from datetime import datetime
from datetime import timezone
from decimal import Decimal


def strtobool(v):
    return v.lower() == 'true'


def strtodate(v):
    return date.fromisoformat(v)


def strtots(v):
    return datetime.fromisoformat(v).astimezone(timezone.utc).replace(tzinfo=None)

def strtots_no_tz(v):
    return datetime.fromisoformat(v)

def check(args):
    tc = unittest.TestCase()
    spark = (SparkSession.builder.remote(args.sparkurl)
             .getOrCreate())

    sql = args.sql
    print(f"Executing sql: {sql}")
    df = spark.sql(sql).collect()
    for row in df:
        print(row)

    with open(args.file, newline='') as insert_csv_file:
        csv_result = list(csv.reader(insert_csv_file))
        tc.assertEqual(len(df), len(csv_result))
        if len(df) == 0:
            return
        if args.partition:
            actual = []
            expected = [] 
            for i in range(len(df[0][0].asDict())):
                actual.append([])
                expected.append([])
            for (row1, row2) in zip(df, csv_result):
                print(f"Row1: {row1}\nRow2: {row2}")
                for i in range(len(row1[0].asDict())):
                    if isinstance(row1[0][i],date):
                        actual[i].append(row1[0][i])
                        expected[i].append(strtodate(row2[i]))
                    elif isinstance(row1[0][i],int):
                        actual[i].append(int(row1[0][i]))
                        expected[i].append(int(row2[i]))
            for i in range(len(actual)):
                actual[i].sort()
                expected[i].sort()
                tc.assertEqual(actual[i], expected[i])
        else:
            for (row1, row2) in zip(df, csv_result):
                print(f"Row1: {row1}\nRow2: {row2}")
                tc.assertEqual(row1[0], int(row2[0]))
                tc.assertEqual(row1[1], int(row2[1]))
                tc.assertEqual(row1[2], int(row2[2]))
                tc.assertEqual(round(row1[3], 5), round(float(row2[3]), 5))
                tc.assertEqual(round(row1[4], 5), round(float(row2[4]), 5))
                tc.assertEqual(row1[5], row2[5])
                tc.assertEqual(row1[6], strtobool(row2[6]))
                tc.assertEqual(row1[7], strtodate(row2[7]))
                tc.assertEqual(row1[8], strtots(row2[8]))
                tc.assertEqual(row1[9], Decimal(row2[9]))
                tc.assertEqual(row1[10], strtots_no_tz(row2[10]))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test icelake with spark')
    parser.add_argument('-s', dest='sparkurl', type=str, help='Spark remote url')
    parser.add_argument("-f", dest='file', type=str, help='Path to query csv file')
    parser.add_argument('--partition', dest='partition', action='store_true', help='Set flag to check partition')
    parser.add_argument("-q", dest='sql', type=str, help='query sql')
    check(parser.parse_args())
