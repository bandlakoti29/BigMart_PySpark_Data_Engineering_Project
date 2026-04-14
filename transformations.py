from pyspark.sql.functions import col, when, avg, sum as _sum

# -----------------------------
# 1. Data Cleaning
# -----------------------------
def clean_data(df):

    # Fill missing Item_Weight with average
    avg_weight = df.select(avg(col("Item_Weight"))).collect()[0][0]
    df = df.fillna({"Item_Weight": avg_weight})

    # Normalize Item_Fat_Content
    df = df.withColumn(
        "Item_Fat_Content",
        when(col("Item_Fat_Content").isin("LF", "low fat"), "Low Fat")
        .when(col("Item_Fat_Content") == "reg", "Regular")
        .otherwise(col("Item_Fat_Content"))
    )

    # Fill Outlet_Size missing values
    df = df.fillna({"Outlet_Size": "Medium"})

    return df


# -----------------------------
# 2. Feature Engineering
# -----------------------------
def feature_engineering(df):

    # Outlet Age
    df = df.withColumn(
        "Outlet_Age",
        2026 - col("Outlet_Establishment_Year")
    )

    # Price Category
    df = df.withColumn(
        "Price_Category",
        when(col("Item_MRP") < 70, "Low")
        .when((col("Item_MRP") >= 70) & (col("Item_MRP") < 150), "Medium")
        .otherwise("High")
    )

    return df


# -----------------------------
# 3. Aggregations (Business Insights)
# -----------------------------
def aggregate_data(df):

    df_agg = df.groupBy("Outlet_Type").agg(
        _sum("Item_Outlet_Sales").alias("Total_Sales"),
        avg("Item_Outlet_Sales").alias("Avg_Sales")
    )

    return df_agg
