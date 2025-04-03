from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, desc


def analyze_sales():
    spark = SparkSession.builder.appName("EcommerceSalesAnalysis").getOrCreate()

    # Read data from Delta table (ensure the table is registered in Spark SQL)
    df = spark.read.format("delta").load("/mnt/data/delta/ecommerce_sales_cleaned")

    # Total revenue per year
    revenue_per_year = df.groupBy("year").agg(sum("price").alias("total_revenue"))
    revenue_per_year.show()

    # Top 10 products by revenue
    top_products = df.groupBy("product_id").agg(sum("price").alias("product_revenue")).orderBy(
        desc("product_revenue")).limit(10)
    top_products.show()

    spark.stop()


if __name__ == "__main__":
    analyze_sales()
