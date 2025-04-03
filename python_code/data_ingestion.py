import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_replace


def ingest_data(raw_data_path, delta_table_path):
    spark = SparkSession.builder.appName("EcommerceDataIngestion").getOrCreate()

    # Read raw CSV data
    df_raw = spark.read.option("header", "true").csv(raw_data_path)

    # Data cleaning and transformations
    df_cleaned = (
        df_raw.withColumn("sale_date", to_date(col("sale_date"), "MM/dd/yyyy"))
        .withColumn("price", regexp_replace(col("price"), "[$,]", "").cast("float"))
        .dropna(subset=["sale_date", "price", "product_id"])
    )
    df_cleaned = df_cleaned.withColumn("year", col("sale_date").substr(1, 4).cast("int"))

    # Write cleaned data as Delta table
    df_cleaned.write.mode("overwrite").format("delta").save(delta_table_path)
    print("Data ingestion and cleaning complete.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python data_ingestion.py <raw_data_path> <delta_table_path>")
        sys.exit(-1)

    raw_data_path = sys.argv[1]
    delta_table_path = sys.argv[2]
    ingest_data(raw_data_path, delta_table_path)
