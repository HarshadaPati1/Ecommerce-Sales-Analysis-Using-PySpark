import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, desc, count


def visualize_data():
    spark = SparkSession.builder.appName("DataVisualization").getOrCreate()

    # Read the cleaned data from the Delta table
    # Ensure that the Delta table exists at the given path.
    delta_path = "/mnt/data/delta/ecommerce_sales_cleaned"
    df = spark.read.format("delta").load(delta_path)

    # --- Plot 1: Total Revenue per Year ---
    revenue_df = df.groupBy("year").agg(sum("price").alias("total_revenue")).orderBy("year")
    pandas_revenue = revenue_df.toPandas()

    plt.figure(figsize=(8, 6))
    plt.bar(pandas_revenue['year'].astype(str), pandas_revenue['total_revenue'], color='skyblue')
    plt.title("Total Revenue per Year")
    plt.xlabel("Year")
    plt.ylabel("Total Revenue")
    plt.tight_layout()
    plt.savefig("total_revenue_per_year.png")
    plt.show()

    # --- Plot 2: Top 10 Products by Revenue ---
    top_products_df = df.groupBy("product_id").agg(sum("price").alias("total_revenue")).orderBy(
        desc("total_revenue")).limit(10)
    pandas_top_products = top_products_df.toPandas()

    plt.figure(figsize=(10, 6))
    plt.bar(pandas_top_products['product_id'], pandas_top_products['total_revenue'], color='salmon')
    plt.title("Top 10 Products by Revenue")
    plt.xlabel("Product ID")
    plt.ylabel("Total Revenue")
    plt.tight_layout()
    plt.savefig("top_10_products_by_revenue.png")
    plt.show()

    # --- Plot 3: Sales Count by Category ---
    category_count_df = df.groupBy("category").agg(count("order_id").alias("order_count")).orderBy("order_count")
    pandas_category_count = category_count_df.toPandas()

    plt.figure(figsize=(10, 6))
    plt.bar(pandas_category_count['category'], pandas_category_count['order_count'], color='lightgreen')
    plt.title("Sales Count by Category")
    plt.xlabel("Category")
    plt.ylabel("Number of Orders")
    plt.tight_layout()
    plt.savefig("sales_count_by_category.png")
    plt.show()

    spark.stop()


if __name__ == "__main__":
    visualize_data()
