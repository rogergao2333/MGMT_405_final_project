# MGMT 405 Final Project Team 21
# Project: Airbnb and Nearby Restaurants’ Yelp Reviews

Authors: Yilin Chen, Roger Gao, Owen Sun, Qingyang Wang, Yuanhang Zhang

## Introduction to the problem
Our project explores the relationship between Airbnb listings and nearby restaurants in New Orleans using Yelp reviews. We integrate Airbnb and Yelp datasets to create an interactive dashboard and conduct statistical analysis to understand how restaurants quantity and quality affect Airbnb prices.

Key Objectives:
- Interactive Dashboard : Helps users visualize nearby restaurant options before booking an Airbnb.
Provides insights into restaurant ratings, top dining choices, and cuisine trends around Airbnbs.
- Analysis of Yelp Ratings’ Impact on Airbnb Performance : Investigates how the quantity and quality of nearby restaurants affects Airbnb pricing.
Uses regression modeling to control for external factors and identify trends.

Technical Approach:
- Data Processing in Spark: Cleaning, feature engineering.
- Storage & Querying: Perform geospatial join on data and store in DuckDB for efficient analysis.
- Visualization: An interactive Tableau dashboard allows users to explore Airbnb-restaurant relationships dynamically.
This project not only enhances user decision-making for Airbnb bookings but also provides business insights for Airbnb hosts and restaurant owners, potentially influencing marketing and pricing strategies.

1.The Data
Part one – Airbnb data (Link: https://insideairbnb.com/get-the-data/)
![image](https://github.com/user-attachments/assets/0dc032f8-8be6-4f7b-b5f5-782ec928c4f0)
On the Inside Airbnb website, download the first data file for New Orleans, listings.csv.gz.

Part two – Yelp data (Link : https://business.yelp.com/data/resources/open-dataset/)
![image](https://github.com/user-attachments/assets/c9728089-119e-41eb-9040-02c6ee403ae1)
On the yelp for business website, download the JSON files and we choose to use the yelp_academic_dataset_business.json file.

2.Pipeline

Then run,
bash pipeline.sh.
