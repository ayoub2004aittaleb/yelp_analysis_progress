%pyspark
#Library

from pyspark import HiveContext
from pyspark.sql.functions import count, col ,avg, countDistinct, desc, lit,sum


hc = HiveContext(sc)
df = hc.table('business')

%pyspark
	#1.Identify the most common merchants in the United States (Top 20).

%pyspark

result = df.groupBy('name')\
    .agg(count('name').alias('cmt'))\
    .orderBy(col('cmt').desc())\
    .limit(20)

z.show(result)

%pyspark
	# Printing the Schema of the data

%pyspark

df.printSchema()

%pyspark
	# Showing the data 

%pyspark

df.show()
%pyspark

df.show()
%pyspark
	#2.Finding the top 10 cities with the most merchants in the United States.
%pyspark

rslt = df.groupBy('city') \
    .agg(count('city').alias('merchant_count')) \
    .orderBy(col('merchant_count').desc()) \
    .limit(10)

z.show(rslt)

%pyspark
	#3.Identify the top 5 states with the most merchants in the United States.
%pyspark

merchants_df = spark.table('business')

state_merchant_counts = merchants_df.groupBy('state').count()
top_states_with_most_merchants = state_merchant_counts.orderBy(col('count').desc())
top_5_states = top_states_with_most_merchants.limit(5)

z.show(top_5_states)
%pyspark
	#4.Finding the most common merchants in the United States and display their average ratings (Top 20).

%pyspark


result = df.groupBy('name') \
           .agg(count('*').alias('count'), avg('stars').alias('avg_rating')) \
           .orderBy(col('count').desc()) \
           .limit(20)

z.show(result)

%pyspark
	#5.Identify and list the top 10 categories with the highest frequency.
%pyspark
avg_ratings = df.groupBy('city').agg(avg('stars').alias('avg_rating'))

top_10 = avg_ratings.orderBy(col('avg_rating').desc()).limit(10)
top_10.show()

%pyspark
	#6. Counting the number of categories.
%pyspark

num_categories = df.select(countDistinct('categories').alias('Number_categories'))
num_categories.show()

%pyspark
	#7. Identifying and listing the top 10 categories with the highest frequency
%pyspark

category_High_fr = df.groupBy('categories') \
                    .count() \
                    .orderBy(desc('count')) \
                    .limit(10)
z.show(category_High_fr)                    
%pyspark
	#8. List the top 20 merchants with the most five-star reviews.
%pyspark

five_star_merchants = df.filter(col('stars') == 5)

top_20 = five_star_merchants.groupBy('name').count()\
            .orderBy(col('count').desc())\
            .limit(20)
z.show(top_20)
%pyspark
	#9.Summarize the types and quantities of restaurants for different cuisines (Chinese, American, Mexican).
%pyspark

restaurant_data = spark.table('business')

american_restaurants = restaurant_data.filter(restaurant_data.categories.contains('American'))
mexican_restaurants = restaurant_data.filter(restaurant_data.categories.contains('Mexican'))
chinese_restaurants = restaurant_data.filter(restaurant_data.categories.contains('Chinese'))

american_summary = american_restaurants.groupBy('categories').count()
mexican_summary = mexican_restaurants.groupBy('categories').count()
chinese_summary = chinese_restaurants.groupBy('categories').count()

# Add 'cuisine' column
american_summary = american_summary.withColumn('cuisine', lit('American')).limit(30)
mexican_summary = mexican_summary.withColumn('cuisine', lit('Mexican')).limit(30)
chinese_summary = chinese_summary.withColumn('cuisine', lit('Chinese')).limit(30)

z.show(american_summary)
z.show(mexican_summary)
z.show(chinese_summary)
	#10 .Explore the distribution of ratings for restaurants of different cuisines.

%pyspark
from pyspark.sql.functions import avg, min, max, stddev

restaurant_data = spark.table('business')
rating_distribution_by_cuisine = restaurant_data.groupBy('cuisine') \
                                               .agg(avg('stars').alias('average_rating'),min('stars').alias('min_rating'),max('stars').alias('max_rating'))

rating_distribution_by_cuisine.show()