from pyspark.sql import SparkSession
from transformations import clean_data, feature_engineering, aggregate_data

# Create Spark Session
spark = SparkSession.builder \
    .appName("BigMart Data Engineering Project") \
    .getOrCreate()

# Load Data
df = spark.read.csv("data/BigMart Sales.csv", header=True, inferSchema=True)

print("Initial Schema:")
df.printSchema()

# Step 1: Clean Data
df_clean = clean_data(df)

# Step 2: Feature Engineering
df_featured = feature_engineering(df_clean)

# Step 3: Aggregations
df_agg = aggregate_data(df_featured)

# Show Results
df_agg.show()

# Save Output
df_agg.write.mode("overwrite").parquet("output/bigmart_sales_analysis")

print("✅ Data pipeline executed successfully!")

spark.stop()