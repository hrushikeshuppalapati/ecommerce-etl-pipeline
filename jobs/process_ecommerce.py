from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType

def main():
    """
    Main function for the Spark job.
    """
    # Initialize Spark Session with PostgreSQL JDBC driver
    spark = SparkSession.builder \
        .appName("EcommerceETL") \
        .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.5.0.jar") \
        .getOrCreate()

    print("Spark session created successfully.")
    # Define schemas for our CSV files
    customer_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True)
    ])

    order_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("order_date", DateType(), True),
        StructField("amount", DoubleType(), True)
    ])

    review_schema = StructType([
        StructField("review_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("review_date", DateType(), True),
        StructField("rating", IntegerType(), True)
    ])

    # Read data from the mounted /data volume
    try:
        customers_df = spark.read.csv("/opt/bitnami/spark/data/customers.csv", header=True, schema=customer_schema)
        orders_df = spark.read.csv("/opt/bitnami/spark/data/orders.csv", header=True, schema=order_schema)
        reviews_df = spark.read.csv("/opt/bitnami/spark/data/reviews.csv", header=True, schema=review_schema)
        
        print("Dataframes created from CSVs.")
        customers_df.printSchema()
        orders_df.printSchema()
        reviews_df.printSchema()

    except Exception as e:
        print(f"Error reading CSV files: {e}")
        spark.stop()
        return
    # Join customers and orders
    customer_orders_df = customers_df.join(orders_df, "customer_id", "left")

    # Aggregate by customer: total spent and total orders
    customer_summary_df = customer_orders_df.groupBy("customer_id", "name", "email") \
        .agg(
            count("order_id").alias("total_orders"),
            col("amount").alias("total_spent")
        ) \
        .na.fill(0)

    # Aggregate reviews: average rating per product
    product_reviews_df = reviews_df.groupBy("product_id") \
        .agg(
            avg("rating").alias("average_rating")
        )

    # For our final table, let's just use the customer summary
    final_df = customer_summary_df

    print("Data transformation complete. Final DataFrame schema:")
    final_df.printSchema()
    final_df.show()
    # Write the final DataFrame to PostgreSQL
    try:
        final_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres-db:5432/ecommerce_db") \
            .option("dbtable", "customer_sales_summary") \
            .option("user", "admin") \
            .option("password", "admin") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        print("Successfully wrote data to PostgreSQL.")

    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")
    
    finally:
        spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    main()
