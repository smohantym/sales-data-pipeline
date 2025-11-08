import argparse, re
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

def to_snake(s: str) -> str:
    s = re.sub(r"[^0-9A-Za-z]+", "_", s.strip())
    s = re.sub(r"_+", "_", s)
    return s.strip("_").lower()

def flexible_date(col):
    fmts = ["yyyy-MM-dd","dd-MM-yyyy","MM/dd/yyyy","yyyy/MM/dd","dd/MM/yyyy"]
    expr = F.to_date(col, fmts[0])
    for fmt in fmts[1:]:
        expr = F.coalesce(expr, F.to_date(col, fmt))
    return F.coalesce(expr, F.to_date(col))

def numeric_from(col):
    s = F.regexp_replace(F.trim(col.cast("string")), r"[₹$£€,\s]", "")
    return F.when(F.length(s) == 0, None).otherwise(s.cast("double"))

def main(input_path: str, output_path: str):
    spark = (SparkSession.builder.appName("SalesETL")
             .config("spark.sql.session.timeZone","UTC").getOrCreate())

    df_raw = (spark.read.option("header",True)
              .option("inferSchema",True)
              .option("mode","PERMISSIVE")
              .csv(f"{input_path}/*.csv"))

    df = df_raw.select([F.col(c).alias(to_snake(c)) for c in df_raw.columns])

    aliases = {
        "sale_id":["sale_id","sales_id","order_id","invoice_id"],
        "order_date":["order_date","date","order_dt","invoice_date"],
        "customer_id":["customer_id","cust_id"],
        "customer_name":["customer_name","customer","client_name"],
        "product_id":["product_id","sku","item_id"],
        "product_name":["product_name","product","item_name"],
        "quantity":["quantity","qty"],
        "unit_price":["unit_price","price","unitprice","unit_amt"],
        "currency":["currency","curr"],
    }
    def first(cols, cands):
        for c in cands:
            if c in cols: return c
        return None

    cols = df.columns
    s_sale  = first(cols, aliases["sale_id"])
    s_date  = first(cols, aliases["order_date"])
    s_cid   = first(cols, aliases["customer_id"])
    s_cname = first(cols, aliases["customer_name"])
    s_pid   = first(cols, aliases["product_id"])
    s_pname = first(cols, aliases["product_name"])
    s_qty   = first(cols, aliases["quantity"])
    s_price = first(cols, aliases["unit_price"])
    s_curr  = first(cols, aliases["currency"])

    df_norm = df.select(
        (F.col(s_sale) if s_sale else F.lit(None)).alias("sale_id"),
        (flexible_date(F.col(s_date)) if s_date else F.lit(None)).alias("order_date"),
        (F.lower(F.trim(F.col(s_cid))) if s_cid else F.lit(None)).alias("customer_id"),
        (F.initcap(F.col(s_cname)) if s_cname else F.lit(None)).alias("customer_name"),
        (F.lower(F.trim(F.col(s_pid))) if s_pid else F.lit(None)).alias("product_id"),
        (F.col(s_pname) if s_pname else F.lit(None)).alias("product_name"),
        (numeric_from(F.col(s_qty)) if s_qty else F.lit(None)).alias("quantity"),
        (numeric_from(F.col(s_price)) if s_price else F.lit(None)).alias("unit_price"),
        (F.lower(F.trim(F.col(s_curr))) if s_curr else F.lit(None)).alias("currency"),
    ).withColumn("amount",(F.col("quantity")*F.col("unit_price")).cast("double")
    ).withColumn("month", F.date_format(F.col("order_date"),"yyyy-MM"))

    if s_sale:
        win = Window.partitionBy("sale_id").orderBy(F.col("order_date").desc_nulls_last())
        df_dedup = df_norm.withColumn("rn", F.row_number().over(win)).where("rn=1").drop("rn")
    else:
        df_dedup = df_norm.dropDuplicates(["order_date","customer_id","product_id","quantity","unit_price"])

    df_clean = df_dedup.filter(
        F.col("order_date").isNotNull() & (F.col("quantity")>0) & (F.col("unit_price")>=0)
    )

    clean_path = f"{output_path}/cleaned_parquet"
    df_clean.write.mode("overwrite").partitionBy("month").parquet(clean_path)

    monthly_revenue = (df_clean.groupBy("month")
                       .agg(F.round(F.sum("amount"),2).alias("revenue"))
                       .orderBy("month"))
    top_customers = (df_clean.groupBy("customer_id","customer_name")
                     .agg(F.round(F.sum("amount"),2).alias("revenue"))
                     .orderBy(F.col("revenue").desc()))

    monthly_revenue.write.mode("overwrite").parquet(f"{output_path}/analytics/monthly_revenue_parquet")
    top_customers.write.mode("overwrite").parquet(f"{output_path}/analytics/top_customers_parquet")
    (monthly_revenue.coalesce(1).write.mode("overwrite").option("header",True)
        .csv(f"{output_path}/analytics/monthly_revenue_csv"))
    (top_customers.coalesce(1).write.mode("overwrite").option("header",True)
        .csv(f"{output_path}/analytics/top_customers_csv"))

    print("\n=== Monthly Revenue ==="); monthly_revenue.show(50, truncate=False)
    print("\n=== Top Customers (Top 25) ==="); top_customers.show(25, truncate=False)
    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input",  dest="input_path",  required=True)
    p.add_argument("--output", dest="output_path", required=True)
    args = p.parse_args()
    main(args.input_path, args.output_path)
