CREATE SCHEMA s1
WITH (location='s3://icebergdata/s1');

CREATE TABLE t1 (
    c1 INTEGER,
    c2 VARCHAR,
    c3 DOUBLE
)
WITH (
    format = 'PARQUET',
    location = 's3://icebergdata/s1/t1/'
);