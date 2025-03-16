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

## 1.The Data

Part one – Airbnb data (Link: https://insideairbnb.com/get-the-data/)
![image](https://github.com/user-attachments/assets/0dc032f8-8be6-4f7b-b5f5-782ec928c4f0)
On the Inside Airbnb website, download the first data file for New Orleans, listings.csv.gz.

Part two – Yelp data (Link : https://business.yelp.com/data/resources/open-dataset/)
![image](https://github.com/user-attachments/assets/c9728089-119e-41eb-9040-02c6ee403ae1)
On the yelp for business website, download the JSON files and we choose to use the yelp_academic_dataset_business.json file.

## 2.Pipeline

Make sure Data and codes are in the following format:  
\data  
  listings17.csv
  yelp1.json
\code  
  airbnb_cleaning.py
  yelp_cleaning.py
  spatial_join.sql
  pipeline.sh

And then run the following line in the repository that contains all the above files

bash pipeline.sh

The above code should create an output directory and should give you four files in the following structure:  
\output  
<blockquote>	airbnb_cleaned_final.csv  </blockquote>
<blockquote> airbnb_yelp.db  </blockquote>
<blockquote> airbnb_yelp_distances.csv  </blockquote>
<blockquote> Yelp_cleaned_final.csv </blockquote>

## 3.Visualization

Please click here to access our dashboard: https://public.tableau.com/app/profile/yilin.chen7785/viz/NewOrleansAirbnbandYelprestaurantAnalysis/Dashboard1
![image](https://github.com/user-attachments/assets/bbd46987-e6e5-4516-b990-ebeacd346d41)

Use guide:
- Input the Airbnb Name you are interested in booking:

![image](https://github.com/user-attachments/assets/7707d6b5-fd26-4421-9260-adfc51d32343)

- Adjust the radius distance you desire (kilometer as unit):

![image](https://github.com/user-attachments/assets/a440fa36-5526-47fa-9ef0-cb78af4c1ad0)

- View the panel about the restaurants nearby:

![image](https://github.com/user-attachments/assets/6bb73b89-156d-4584-bbc0-8d25058ecc24)

- Filter the restaurant category you desire (China cuisine & Cafe, for example):

![image](https://github.com/user-attachments/assets/8189487c-be3a-4f66-b2ee-77fa1fcc0dda)

![image](https://github.com/user-attachments/assets/75593558-8bc0-4efc-94ee-7ce730f9be22)

- Input another candidate Airbnb and query again!

## 4.Summary of Analysis

This analysis utilizes python to explore the relationship between Airbnb pricing and restaurant availability across different neighborhoods. The key takeaways from the study are as follows:

- Overall, Airbnb prices tend to increase when nearby restaurants have higher average ratings. Guests are willing to pay a premium for Airbnbs located near well-rated dining options, likely due to the added convenience and quality of local experiences.
- The number of nearby restaurants also has a strong positive correlation with Airbnb pricing. This suggests that in many areas, denser restaurant clusters make a location more attractive to guests, increasing Airbnb prices.
- However, this relationship varies by neighborhood. While some neighborhoods show a strong positive correlation between restaurant density and Airbnb prices, others exhibit a weak or no correlation. This indicates that factors beyond restaurant availability—such as business districts, accessibility, and local attractions—may also influence Airbnb pricing.

Neighborhoods with Strong Positive Correlation

The following neighborhoods show statistically significant positive correlations between the number of nearby restaurants and Airbnb prices. These areas may benefit most from restaurant-driven pricing strategies:
![image](https://github.com/user-attachments/assets/1311618f-4d14-4f9c-86ac-404e30579015)

## 5.Business Insights & Implications

- For Airbnb Hosts: Hosts in the above neighborhoods may consider emphasizing proximity to restaurants in their listings and potentially adjusting pricing based on the presence of high-rated dining options.
- For Restaurant Owners: Investing in locations within these neighborhoods could be beneficial, as areas with strong Airbnb activity may attract more customers.
- For City Planners & Investors: Understanding which neighborhoods see a direct impact from restaurant density on Airbnb pricing can help guide investment decisions, tourism development, and zoning regulations.
- Conclusion: While restaurant availability generally enhances Airbnb pricing, its influence varies across neighborhoods. Targeting the right locations—where restaurant density strongly correlates with Airbnb demand—can help optimize pricing strategies and business decisions in the hospitality and food sectors.

