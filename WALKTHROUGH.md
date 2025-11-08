# What each file does

## 1) `docker-compose.yml`

Three containers:

1. **spark-master**

* Runs the Spark Master process (UI at `http://localhost:8080`).
* Mounts your host data folder into the container so Spark can read/write files:

  * `${DATA_DIR} → /opt/spark-data` (inputs live here under `/input`)
  * `${DATA_DIR}/output → /opt/spark-output` (outputs land here)

2. **spark-worker**

* A Spark Worker that actually runs executor tasks.
* Mounts the **same** host paths so executors can see the same files. This is essential because you read via `file:/opt/spark-data/...`.

3. **spark-etl** (the driver that submits your job)

* Uses the same `apache/spark:3.5.1-python3` image (no custom build needed).
* Mounts the same data/output volumes and also mounts your **app code**:

  * `./spark-app → /opt/spark-app`
* `entrypoint` does four things:

  1. displays input files (`ls -l "$INPUT_PATH"`) for quick sanity check
  2. ensures Ivy cache & output dirs exist
  3. `chmod -R 777` on the output dir (avoids permission headaches)
  4. runs `spark-submit` pointing to `/opt/spark-app/app.py` with `--input/--output`

### Key lines (compose)

```yaml
volumes:
  - ${DATA_DIR}:/opt/spark-data          # shared input root
  - ${DATA_DIR}/output:/opt/spark-output # shared output root
...
- ./spark-app:/opt/spark-app             # mounts your app.py
...
/opt/spark/bin/spark-submit ... /opt/spark-app/app.py --input "$INPUT_PATH" --output "$OUTPUT_PATH"
```

Those mounts + the exact same inside-container paths across **master/worker/etl** are what make `file:/...` reads work on a cluster.

---

## 2) `.env`

```env
DATA_DIR=./data
INPUT_PATH=/opt/spark-data/input
OUTPUT_PATH=/opt/spark-output
```

* `DATA_DIR` can be relative (as you have) or absolute; relative is fine on Docker Desktop.
* Spark ETL uses `INPUT_PATH` and `OUTPUT_PATH` from the environment in `entrypoint`.

---

## 3) `spark-app/Dockerfile` (optional for now)

You included a minimal Dockerfile for copying `app.py` into an image. Since you’re bind-mounting `./spark-app`, this file isn’t required in the current flow, but it’s handy if you later want to ship a prebuilt image (e.g., with extra Python libs).

---

## 4) `spark-app/requirements.txt` (not used in current flow)

Same story—useful only if you decide to build a custom image and install extra libs. Your current ETL doesn’t need pandas/dateutil/pyarrow because you’re using **Spark SQL functions** and Spark’s built-in CSV/Parquet readers/writers.

---

## 5) `spark-app/app.py`

This is the heart of your ETL.

### High-level flow

1. Start Spark session (UTC timezone).
2. Read all `*.csv` from `INPUT_PATH` with header + schema inference.
3. Normalize column names to `snake_case`.
4. Flexibly map “probable” columns (e.g., `sales_id` → `sale_id`) via alias lists.
5. Cleanse/standardize:

   * dates: `flexible_date()` tries common patterns and falls back to Spark’s default
   * numerics: strip currency symbols/spaces/commas, cast to `double`
   * text: trim+lower for IDs; `initcap` for customer names
   * compute `amount = quantity * unit_price`
   * derive `month = yyyy-MM` for partitioning/analytics
6. De-duplicate:

   * if `sale_id` exists → keep the **latest** record per `sale_id` by `order_date`
   * else → drop duplicates on a composite key
7. Filter out bad rows: null dates, non-positive quantities, negative prices.
8. Write **cleaned** dataset partitioned by `month` (Parquet).
9. Compute analytics:

   * `monthly_revenue` (sum of `amount` by `month`)
   * `top_customers` (sum of `amount` by `customer_id, customer_name`, desc)
10. Write analytics in **both** Parquet and single-file CSV form.
11. Show results to console and stop the session.

### Key helper functions

```python
def to_snake(s):
    # Converts headers like "Order Date" -> "order_date"
```

```python
def flexible_date(col):
    # Tries: yyyy-MM-dd, dd-MM-yyyy, MM/dd/yyyy, yyyy/MM/dd, dd/MM/yyyy
    # Then coalesces to the first successful parse, else Spark's default.
```

```python
def numeric_from(col):
    # Strips ₹ $ £ € , space; casts to double; empty -> null
```

### Core transform

```python
df = df_raw.select([F.col(c).alias(to_snake(c)) for c in df_raw.columns])

# pick the first matching source column per logical field
s_sale  = first(cols, aliases["sale_id"])
...
df_norm = df.select(
  (F.col(s_sale)  if s_sale  else F.lit(None)).alias("sale_id"),
  (flexible_date(F.col(s_date)) if s_date else F.lit(None)).alias("order_date"),
  (F.lower(F.trim(F.col(s_cid))) if s_cid else F.lit(None)).alias("customer_id"),
  ...
).withColumn("amount", (F.col("quantity")*F.col("unit_price")).cast("double")
).withColumn("month", F.date_format(F.col("order_date"), "yyyy-MM"))
```

### Dedup logic (fast, simple)

```python
if s_sale:
    win = Window.partitionBy("sale_id").orderBy(F.col("order_date").desc_nulls_last())
    df_dedup = df_norm.withColumn("rn", F.row_number().over(win)).where("rn=1").drop("rn")
else:
    df_dedup = df_norm.dropDuplicates(["order_date","customer_id","product_id","quantity","unit_price"])
```

### Outputs (directory structure)

* Cleaned:

  * `${DATA_DIR}/output/cleaned_parquet/` partitioned by `month=YYYY-MM`
* Analytics (Parquet):

  * `${DATA_DIR}/output/analytics/monthly_revenue_parquet/`
  * `${DATA_DIR}/output/analytics/top_customers_parquet/`
* Analytics (CSV, one file per dataset):

  * `${DATA_DIR}/output/analytics/monthly_revenue_csv/part-*.csv`
  * `${DATA_DIR}/output/analytics/top_customers_csv/part-*.csv`

---

# Runtime workflow (step-by-step)

1. **Prepare data**
   Put CSVs under `./data/input/` (relative to your compose project). Your sample file is already there.

2. **Start the ETL**

   ```
   docker compose up --build spark-etl
   ```

   Compose starts master/worker (if not already), then the `spark-etl` driver.

3. **Driver submits job**
   The `entrypoint` prints the contents of `/opt/spark-data/input`, then calls `spark-submit`.

4. **Spark reads CSV**
   Because master/worker/etl all mount `${DATA_DIR} → /opt/spark-data`, executors can read `file:/opt/spark-data/input/*.csv` locally without HDFS/S3.

5. **Transform + write**
   ETL runs the normalization/clean/dedup/analytics steps and writes Parquet + CSV to `/opt/spark-output/...` (which is `${DATA_DIR}/output` on your host).

6. **View results**

   * Console shows the `monthly_revenue` and `top_customers` tables.
   * Files are in `./data/output/...`.