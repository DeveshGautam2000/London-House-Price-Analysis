from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, lit, row_number, when
from pyspark.sql.window import Window

# Initialize SparkSession
spark = (SparkSession.builder
    .appName("LondonHousePriceAnalysis")
    .enableHiveSupport()
    .getOrCreate())

# Load the CSV file into a Spark DataFrame
file_path = "london_house_price_data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)
df = df.withColumnRenamed('saleEstimate_valueChange.percentageChange', 'saleEstimate_percentageChange')
# print(df.select('saleEstimate_percentageChange').take(2))
# print(df.printSchema())

# Query 1: Postcode with the Highest Average Sale Price
postcode_avg_price = df.groupBy("postcode").agg(avg("saleEstimate_currentPrice").alias("avg_sale_price"))
highest_avg_sale_postcode = postcode_avg_price.orderBy(col("avg_sale_price").desc()).first()
print("Postcode with the highest average sale price:", highest_avg_sale_postcode["postcode"])
print("Highest average sale price:", highest_avg_sale_postcode["avg_sale_price"])

# Query 2: Property Type with the Highest Average Number of Bathrooms
property_avg_bathrooms = df.groupBy("propertyType").agg(avg("bathrooms").alias("avg_bathrooms"))
highest_avg_bathrooms_type = property_avg_bathrooms.orderBy(col("avg_bathrooms").desc()).first()
print("Property type with the highest average number of bathrooms:", highest_avg_bathrooms_type["propertyType"])
print("Highest average number of bathrooms:", highest_avg_bathrooms_type["avg_bathrooms"])

# Query 3: Total Number of Properties Available in Each Country
country_property_count = df.groupBy("country").agg(count("*").alias("total_properties"))
print("Total number of properties in each country:")
country_property_count.show()

# Query 4: Average Percentage Change in Sale Price for Each Tenure Type
# Handle missing or null values in the percentageChange column
# column_names = df.columns
# print(column_names)
df = df.filter(df['saleEstimate_percentageChange'].isNotNull())

# Correctly reference the column with backticks
tenure_avg_price_change = df.groupBy("tenure").agg(
    avg('saleEstimate_percentageChange').alias("avg_percentage_change")
)
print("Average percentage change in sale price for each tenure type:")
tenure_avg_price_change.show()

# Query 5: Country with the Highest Average Rent Price
# Handle missing or null values in the rentEstimate_currentPrice column
df = df.filter(col("rentEstimate_currentPrice").isNotNull())

country_avg_rent = df.groupBy("country").agg(
    avg("rentEstimate_currentPrice").alias("avg_rent_price")
)
highest_avg_rent_country = country_avg_rent.orderBy(col("avg_rent_price").desc()).first()
print("Country with the highest average rent price:", highest_avg_rent_country["country"])
print("Highest average rent price:", highest_avg_rent_country["avg_rent_price"])

# Query 6: Property Type with the Highest Average Number of Bedrooms
# Handle missing or null values in the bedrooms column
df = df.filter(col("bedrooms").isNotNull())

property_avg_bedrooms = df.groupBy("propertyType").agg(
    avg("bedrooms").alias("avg_bedrooms")
)
highest_avg_bedrooms_type = property_avg_bedrooms.orderBy(col("avg_bedrooms").desc()).first()
print("Property type with the highest average number of bedrooms:", highest_avg_bedrooms_type["propertyType"])
print("Highest average number of bedrooms:", highest_avg_bedrooms_type["avg_bedrooms"])

# Query 7: Median Sale Price for Each Tenure Type
# Median calculation with window function (row_number) for each group

# Step 1: Use a window to rank rows within each tenure group
window_spec = Window.partitionBy("tenure").orderBy("saleEstimate_currentPrice")
ranked_df = df.withColumn("rank", row_number().over(window_spec))

# Step 2: Calculate the median rank for each group
group_counts = df.groupBy("tenure").agg(count("saleEstimate_currentPrice").alias("count"))
median_rank = group_counts.withColumn("median_rank", (col("count") / 2).cast("int"))

# Step 3: Join the median rank with the ranked DataFrame and filter for median rows
median_sale_price = ranked_df.join(median_rank, "tenure").filter(col("rank") == col("median_rank"))
median_sale_price = median_sale_price.select("tenure", "saleEstimate_currentPrice").distinct()

print("Median sale price for each tenure type:")
median_sale_price.show()

# Additional Query: Top 3 Postcodes with the Most Properties Listed
top_postcodes = df.groupBy("postcode").agg(count("*").alias("property_count"))
print("Top 3 postcodes with the most properties listed:")
top_postcodes.orderBy(col("property_count").desc()).show(3)

# Stop the SparkSession
spark.stop()