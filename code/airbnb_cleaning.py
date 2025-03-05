from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ListingsData").getOrCreate()
df = spark.read.option("header", True) \
               .option("inferSchema", True) \
               .option("multiLine", True) \
               .option("escape", "\"") \
               .csv("/home/ubuntu/listings.csv")
df.show(5, truncate=False)
df.select("amenities").show(10, truncate=False)
df_selected = df.select(
    "id", "name", "latitude", "longitude", "price", "neighbourhood_cleansed",
    "host_id", "host_name", "host_response_rate", "host_acceptance_rate", 
    "host_is_superhost", "host_neighbourhood", "host_total_listings_count", 
    "host_identity_verified", "room_type", "accommodates", "bathrooms", 
    "bedrooms", "amenities", "number_of_reviews", "review_scores_rating"
)
df_cleaned = df_selected.filter((df_selected.price.isNotNull()) & (df_selected.price != "NA") & (df_selected.price != ""))
df_cleaned2 = df_cleaned.filter(
    (df_selected.price.isNotNull()) & (df_selected.price != "NA") & (df_selected.price != "") &
    (df_selected.latitude.isNotNull()) & (df_selected.longitude.isNotNull())
)
df_cleaned2.filter(
    df_cleaned.price.isNull() | (df_cleaned.price == "NA") | (df_cleaned.price == "") |
    df_cleaned.latitude.isNull() | df_cleaned.longitude.isNull()
).count()
df_cleaned2.select("price").show(50, truncate=False)
df_cleaned2.printSchema()
from pyspark.sql.functions import regexp_replace, col
df_cleaned2 = df_cleaned2.withColumn("price", regexp_replace(col("price"), "[$,]", "").cast("int"))
df_cleaned2.select("price").show(10, truncate=False)
df_cleaned2.printSchema()
from pyspark.sql.functions import regexp_replace, col, split
# Convert the string representation of list into actual list (array)
df_cleaned2 = df_cleaned2.withColumn("amenities_list", split(regexp_replace(col("amenities"), r'[\[\]"]', ''), ",\s*"))
# Show the result to verify the transformation
df_cleaned2.select("amenities", "amenities_list").show(5, truncate=False)
from pyspark.sql.functions import col, when
# Create dummy columns for Wifi, Air conditioning, and Parking
from pyspark.sql.functions import array_contains
df_cleaned2 = df_cleaned2.withColumn("wifi_dummy", when(array_contains(col("amenities_list"), "Wifi"), 1).otherwise(0)) \ .withColumn("air_conditioning_dummy", when(array_contains(col("amenities_list"), "Air conditioning"), 1).otherwise(0)) \ .withColumn("parking_dummy", when(array_contains(col("amenities_list"), "parking"), 1).otherwise(0))
# Show the results to verify the new dummy columns
df_cleaned2.select("amenities", "amenities_list", "wifi_dummy", "air_conditioning_dummy", "parking_dummy").show(10, truncate=False)
df_cleaned2 = df_cleaned2.filter(
    (col("host_is_superhost").isin("t", "f")) & 
    (col("host_identity_verified").isin("t", "f"))
)
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
indexer = StringIndexer(inputCol="room_type", outputCol="room_type_index")
encoder = OneHotEncoder(inputCol="room_type_index", outputCol="room_type_encoded")
pipeline = Pipeline(stages=[indexer, encoder])
df_cleaned2 = pipeline.fit(df_cleaned2).transform(df_cleaned2)
df_cleaned2.select("room_type", "room_type_index", "room_type_encoded").show(50, truncate=False)
from pyspark.sql.functions import when
df_cleaned2 = df_cleaned2.withColumn("room_type_Entire_home_apt", when(col("room_type") == "Entire home/apt", 1).otherwise(0)) \
                         .withColumn("room_type_Private_room", when(col("room_type") == "Private room", 1).otherwise(0)) \
                         .withColumn("room_type_Shared_room", when(col("room_type") == "Shared room", 1).otherwise(0)) \
                         .withColumn("room_type_Hotel_room", when(col("room_type") == "Hotel room", 1).otherwise(0))
df_cleaned2.select("room_type", "room_type_Entire_home_apt", "room_type_Private_room", "room_type_Shared_room", "room_type_Hotel_room").show(10, truncate=False)
df_cleaned2 = df_cleaned2.drop("room_type_index", "room_type_encoded")
df_cleaned2.write.csv("/home/ubuntu/airbnb_full.csv", mode="overwrite", header=True)
cat /home/ubuntu/airbnb_full.csv/part-* > /home/ubuntu/airbnb.csv
scp -i ~/Downloads/MGMTMSA405.pem ubuntu@35.92.138.7:/home/ubuntu/airbnb.csv ~/Downloads/
