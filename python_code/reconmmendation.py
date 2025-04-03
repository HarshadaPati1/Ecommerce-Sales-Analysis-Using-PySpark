from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import collect_set, explode


def generate_recommendations(product_of_interest="P123"):
    spark = SparkSession.builder.appName("ProductRecommendation").getOrCreate()

    # Read cleaned sales data
    df = spark.read.format("delta").load("/mnt/data/delta/ecommerce_sales_cleaned")

    # Build baskets per order
    df_basket = df.groupBy("order_id").agg(collect_set("product_id").alias("products"))
    df_exploded = df_basket.withColumn("product", explode("products"))

    # Compute product co-occurrence
    df_cooccurrence = df_exploded.alias("a").join(
        df_exploded.alias("b"),
        (F.col("a.order_id") == F.col("b.order_id")) & (F.col("a.product") != F.col("b.product")),
        "inner"
    ).select(F.col("a.product").alias("product"), F.col("b.product").alias("co_product"))

    df_recommendation = df_cooccurrence.groupBy("product", "co_product").count().orderBy("product", F.desc("count"))
    recommendations = df_recommendation.filter(F.col("product") == product_of_interest).limit(5)
    recommendations.show()

    spark.stop()


if __name__ == "__main__":
    generate_recommendations()
