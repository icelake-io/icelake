# Generate Test Data via Spark3

## Setup

To initiate the `spark-sql` shell:

```bash
docker-compose up
docker exec -it spark-iceberg spark-sql
```

## Generate Data

create table

```sql
CREATE TABLE iceberg_ctl.iceberg_db.iceberg_tbl (id INT, data STRING) USING ICEBERG;
```

insert data

```sql
-- First transaction
INSERT INTO iceberg_ctl.iceberg_db.iceberg_tbl VALUES (1, 'a'), (2, 'b'), (3, 'c');
-- Second transaction
INSERT INTO iceberg_ctl.iceberg_db.iceberg_tbl VALUES (4, 'd'), (5, 'e'), (6, 'd');
```
