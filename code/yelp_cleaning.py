#CONFIRMED FINISH

#df = spark.read.json("/home/ubuntu/yelp1.json")
#df_filtered = df.filter(df.city == "New Orleans")
#df_filtered = df_filtered.filter( (df_filtered.categories.contains("Restaurants")) | (df_filtered.categories.contains("Food")) )
#df_selected = df_filtered.select("business_id", "name", "address", "longitude", "latitude", "stars", "review_count", "categories")
#df_selected.show(10, truncate=True)
#df_selected.write.csv("/home/ubuntu/Yelp.csv", mode="overwrite", header=True)
#cat /home/ubuntu/Yelp.csv/part-* > /home/ubuntu/Yelp_final.csv
#scp -i ~/Downloads/MGMTMSA405.pem ubuntu@35.92.138.7:/home/ubuntu/Yelp_final.csv ~/Downloads/


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, expr, udf, lower
from pyspark.sql.types import StringType, ArrayType

# Initialize Spark Session
spark = SparkSession.builder.appName("YelpProcessing").getOrCreate()

# Load Yelp JSON dataset
df = spark.read.json("/home/ubuntu/team21/data/yelp1.json")

# Filter for New Orleans and only restaurant-related businesses
df_filtered = df.filter((col("city") == "New Orleans") & 
                        (col("categories").contains("Restaurants") | col("categories").contains("Food")))

# Select relevant columns
df_selected = df_filtered.select("business_id", "name", "address", "longitude", "latitude", 
                                 "stars", "review_count", "categories")

# Convert categories column to a list
df_selected = df_selected.withColumn("categories_list", split(col("categories"), ", "))

# Define cuisine mapping
cuisine_mapping = {
    "Mexican": "Mexico", "Taco": "Mexico", "Burrito": "Mexico", "Taqueria": "Mexico",
    "Italian": "Italy", "Pizza": "Italy", "Pasta": "Italy", "Trattoria": "Italy",
    "French": "France", "Bistro": "France", "Brasserie": "France", "Patisserie": "France",
    "Spanish": "Spain", "Tapas": "Spain", "Paella": "Spain", "Churros": "Spain",
    "Portuguese": "Portugal", "Nando's": "Portugal",
    "German": "Germany", "Schnitzel": "Germany", "Bratwurst": "Germany",
    "British": "UK", "Fish and Chips": "UK", "Pub": "UK",
    "Greek": "Greece", "Gyro": "Greece", "Souvlaki": "Greece",
    "Russian": "Russia", "Borscht": "Russia", "Pelmeni": "Russia",
    "Chinese": "China", "Dim Sum": "China", "Cantonese": "China",
    "Japanese": "Japan", "Sushi": "Japan", "Ramen": "Japan",
    "Korean": "Korea", "BBQ": "Korea", "Kimchi": "Korea",
    "Thai": "Thailand", "Pad Thai": "Thailand", "Tom Yum": "Thailand",
    "Vietnamese": "Vietnam", "Pho": "Vietnam", "Banh Mi": "Vietnam",
    "Indian": "India", "Curry": "India", "Tandoori": "India",
    "Turkish": "Turkey", "Doner": "Turkey", "Kebab": "Turkey",
    "Middle Eastern": "Middle East", "Falafel": "Middle East",
    "Brazilian": "Brazil", "Churrasco": "Brazil",
    "Peruvian": "Peru", "Ceviche": "Peru",
    "Argentine": "Argentina", "Asado": "Argentina",
    "American": "USA", "Burgers": "USA", "Steakhouse": "USA",
    "Canadian": "Canada", "Poutine": "Canada"
}

# Define food type mapping
food_type_mapping = {
    "Bar": "Bar", "Beer": "Bar", "Pub": "Bar", "Wine": "Bar",
    "Seafood": "Seafood", "Fish": "Seafood", "Crab": "Seafood",
    "Steakhouse": "Steakhouse", "Grill": "Steakhouse",
    "Pizza": "Pizza", "Pizzeria": "Pizza",
    "Ice Cream": "Ice Cream", "Gelato": "Ice Cream",
    "Bakery": "Bakery", "Patisserie": "Bakery", "Boulangerie": "Bakery",
    "Cafe": "Cafe", "Coffee": "Cafe", "Tea": "Cafe",
    "Fast Food": "Fast Food", "Burgers": "Fast Food",
    "Vegetarian": "Vegetarian", "Vegan": "Vegetarian",
    "Dessert": "Dessert", "Chocolate": "Dessert",
    "Asian Fusion": "Asian Fusion", "Fusion": "Asian Fusion"
}

# Define UDFs for categorization
# def categorize_restaurant(category_list):
#     if category_list is not None:
#         for category in category_list:
#             category_lower = category.lower()
#             for keyword, country in cuisine_mapping.items():
#                 if keyword.lower() in category_lower:
#                     return country
#     return "Other"

# def categorize_food_type(category_list, cuisine_country):
#     if cuisine_country == "Other" and category_list is not None:
#         for category in category_list:
#             category_lower = category.lower()
#             for keyword, food_type in food_type_mapping.items():
#                 if keyword.lower() in category_lower:
#                     return food_type
#     return "Other"

from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import StringType

# Define UDF for categorizing by country
def categorize_restaurant(category_list):
    if category_list is not None:
        for category in category_list:
            category_lower = category.lower()
            for keyword, country in cuisine_mapping.items():
                if keyword.lower() in category_lower:
                    return country
    return "Other"

# Define UDF for categorizing by food type
def categorize_food_type(category_list, cuisine_country):
    if cuisine_country == "Other" and category_list is not None:
        for category in category_list:
            category_lower = category.lower()
            for keyword, food_type in food_type_mapping.items():
                if keyword.lower() in category_lower:
                    return food_type
    return "Other"

# Register UDFs
categorize_restaurant_udf = udf(categorize_restaurant, StringType())
categorize_food_type_udf = udf(categorize_food_type, StringType())  # Takes 2 arguments

# Step 1: Categorize by country first
df_selected = df_selected.withColumn("cuisine_country", categorize_restaurant_udf(col("categories_list")))

# Step 2: Categorize by food type (only for rows where cuisine_country == "Other")
df_selected = df_selected.withColumn("food_type", categorize_food_type_udf(col("categories_list"), col("cuisine_country")))

# Step 3: Final category column: Use country if not "Other", otherwise use food type
df_selected = df_selected.withColumn(
    "final_category",
    when(col("cuisine_country") != "Other", col("cuisine_country"))
    .otherwise(col("food_type"))
)

# Show results
# df_selected.select("cuisine_country", "food_type", "final_category").show(truncate=False)




# from pyspark.sql.functions import col, when
# from pyspark.sql.types import StringType
# from pyspark.sql.functions import udf
# # Define UDFs for categorization
# categorize_restaurant_udf = udf(categorize_restaurant, StringType())  # Categorizes by country
# categorize_food_type_udf = udf(categorize_food_type, StringType())    # Categorizes by food type

# # Add separate columns
# df_selected = df_selected.withColumn("cuisine_country", categorize_restaurant_udf(col("categories_list")))
# df_selected = df_selected.withColumn("food_type", categorize_food_type_udf(col("categories_list")))

# # Create final category column: Use country if available, otherwise use food type
# df_selected = df_selected.withColumn(
#     "final_category",
#     when(col("cuisine_country") != "Other", col("cuisine_country"))
#     .otherwise(col("food_type"))
# )

# Show results
#df_selected.show(truncate=False)

from pyspark.sql.functions import col, concat_ws

# Convert categories_list (array) to a comma-separated string
df_selected = df_selected.withColumn("categories_list", concat_ws(", ", col("categories_list")))

# Save as CSV
df_selected.write.csv("/home/ubuntu/Yelp.csv", mode="overwrite", header=True)

# then quit()

#run this line in ec2
#cat /home/ubuntu/Yelp.csv/part-* > /home/ubuntu/Yelp_final.csv

#run this line in local terminal if needed
#scp -i ~/Downloads/MGMTMSA405.pem ubuntu@35.92.138.7:/home/ubuntu/Yelp_final.csv ~/Downloads/
#scp -i ~/Downloads/MGMTMSA405.pem ubuntu@35.92.138.7:/home/ubuntu/Yelp_final.csv ~/Downloads/yelptest2.csv
