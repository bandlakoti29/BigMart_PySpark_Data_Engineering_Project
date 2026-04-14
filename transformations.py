from pyspark.sql.functions import col, when, avg, sum as _sum

# 1. Data Cleaning
def clean_data(df):

    avg_weight = df.select(avg(col("Item_Weight"))).collect()[0][0]
    df = df.fillna({"Item_Weight": avg_weight})

    df = df.withColumn(
        "Item_Fat_Content",
        when(col("Item_Fat_Content").isin("LF", "low fat"), "Low Fat")
        .when(col("Item_Fat_Content") == "reg", "Regular")
        .otherwise(col("Item_Fat_Content"))
    )
    
    df = df.fillna({"Outlet_Size": "Medium"})

    return df


# 2. Feature Engineering
def feature_engineering(df):

    df = df.withColumn(
        "Outlet_Age",
        2026 - col("Outlet_Establishment_Year")
    )

    df = df.withColumn(
        "Price_Category",
        when(col("Item_MRP") < 70, "Low")
        .when((col("Item_MRP") >= 70) & (col("Item_MRP") < 150), "Medium")
        .otherwise("High")
    )

    return df



# 3. Aggregations
def aggregate_data(df):

    df_agg = df.groupBy("Outlet_Type").agg(
        _sum("Item_Outlet_Sales").alias("Total_Sales"),
        avg("Item_Outlet_Sales").alias("Avg_Sales")
    )

    return df_agg
