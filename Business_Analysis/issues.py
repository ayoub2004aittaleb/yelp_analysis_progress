# Import necessary libraries
from pyspark import HiveContext
from pyspark.sql.functions import count, col, avg, countDistinct, desc, lit, sum, min, max, stddev

# Initialize HiveContext
hc = HiveContext(sc)

# Load data from the 'business' table
df = hc.table('business')

# 1. Identify the most common merchants in the United States (Top 20).
top_merchants = df.groupBy('name') \
                  .agg(count('name').alias('merchant_count')) \
                  .orderBy(col('merchant_count').desc()) \
                  .limit(20)

z.show(top_merchants)

# Print the schema of the data
df.printSchema()

# Show the data
df.show()

# 2. Finding the top 10 cities with the most merchants in the United States.
top_cities = df.groupBy('city') \
               .agg(count('city').alias('merchant_count')) \
               .orderBy(col('merchant_count').desc()) \
               .limit(10)

z.show(top_cities)

# 3. Identify the top 5 states with the most merchants in the United States.
top_states = df.groupBy('state') \
               .agg(count('state').alias('merchant_count')) \
               .orderBy(col('merchant_count').desc()) \
               .limit(5)

z.show(top_states)

# 4. Finding the most common merchants in the United States and display their average ratings (Top 20).
top_merchants_avg_ratings = df.groupBy('name') \
                               .agg(count('*').alias('count'), avg('stars').alias('avg_rating')) \
                               .orderBy(col('count').desc()) \
                               .limit(20)

z.show(top_merchants_avg_ratings)

# 5. Identify and list the top 10 categories with the highest frequency.
top_categories = df.groupBy('categories') \
                   .count() \
                   .orderBy(desc('count')) \
                   .limit(10)

z.show(top_categories)

# 6. Counting the number of categories.
num_categories = df.select(countDistinct('categories').alias('Number_categories'))
num_categories.show()

# 7. Identifying and listing the top 10 categories with the highest frequency.
top_categories_high_frequency = df.groupBy('categories') \
                                   .count() \
                                   .orderBy(desc('count')) \
                                   .limit(10)
z.show(top_categories_high_frequency)

# 8. List the top 20 merchants with the most five-star reviews.
top_20_five_star_merchants = df.filter(col('stars') == 5) \
                                .groupBy('name').count() \
                                .orderBy(col('count').desc()) \
                                .limit(20)

z.show(top_20_five_star_merchants)

# 9. Summarize the types and quantities of restaurants for different cuisines (Chinese, American, Mexican).
american_restaurants = df.filter(df.categories.contains('American'))
mexican_restaurants = df.filter(df.categories.contains('Mexican'))
chinese_restaurants = df.filter(df.categories.contains('Chinese'))

american_summary = american_restaurants.groupBy('categories').count().withColumn('cuisine', lit('American')).limit(30)
mexican_summary = mexican_restaurants.groupBy('categories').count().withColumn('cuisine', lit('Mexican')).limit(30)
chinese_summary = chinese_restaurants.groupBy('categories').count().withColumn('cuisine', lit('Chinese')).limit(30)

z.show(american_summary)
z.show(mexican_summary)
z.show(chinese_summary)

# 10. Explore the distribution of ratings for restaurants of different cuisines.
rating_distribution_by_cuisine = df.groupBy('categories') \
                                    .agg(avg('stars').alias('average_rating'), min('stars').alias('min_rating'), max('stars').alias('max_rating'))

z.show(rating_distribution_by_cuisine)
