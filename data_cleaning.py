from pyspark.sql.functions import col, when

def clean_data(df):
    
    df = df.fillna({
        "Item_Weight": 0,
        "Outlet_Size": "Unknown"
    })

    df = df.withColumn(
        "Item_Fat_Content",
        when(col("Item_Fat_Content").isin("LF", "low fat"), "Low Fat")
        .when(col("Item_Fat_Content") == "reg", "Regular")
        .otherwise(col("Item_Fat_Content"))
    )

    df = df.dropDuplicates()

    return df
