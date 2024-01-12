from pyiceberg.catalog import load_catalog
import argparse
import unittest

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test icelake with spark")
    parser.add_argument("-rest", dest="resturl", type=str, help="rest url")
    parser.add_argument("-s3", dest="s3url",type=str, help="s3 url")
    parser.add_argument("-t1", dest="t1", type=str, help="table 1")
    parser.add_argument("-t2", dest="t2", type=str, help="table 2")
    args = parser.parse_args()

    tc = unittest.TestCase()
    catalog = load_catalog(
        **{
            "type": "rest",
            "uri": args.resturl,
            "s3.endpoint": args.s3url,
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )
    t1 = catalog.load_table(f"s1.{args.t1}")
    t2 = catalog.load_table(f"s1.{args.t2}")

    con1 = t1.scan().to_duckdb(table_name="t1")
    con2 = t2.scan().to_duckdb(table_name="t2")

    res1 = con1.execute("SELECT * from t1").fetchall()
    res2 = con2.execute("SELECT * from t2").fetchall()
    res1.sort()
    res2.sort()
    tc.assertEqual(res1, res2)
