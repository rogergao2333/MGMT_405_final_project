df = spark.read.json("/home/ubuntu/yelp1.json")
df_filtered = df.filter(df.city == "New Orleans")
df_filtered = df_filtered.filter( (df_filtered.categories.contains("Restaurants")) | (df_filtered.categories.contains("Food")) )
df_selected = df_filtered.select("business_id", "name", "address", "longitude", "latitude", "stars", "review_count", "categories")
df_selected.show(10, truncate=True)
df_selected.write.csv("/home/ubuntu/Yelp.csv", mode="overwrite", header=True)
cat /home/ubuntu/Yelp.csv/part-* > /home/ubuntu/Yelp_final.csv
scp -i ~/Downloads/MGMTMSA405.pem ubuntu@35.92.138.7:/home/ubuntu/Yelp_final.csv ~/Downloads/
