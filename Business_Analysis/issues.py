from pyspark import HiveContext
from pyspark.sql.functions import count, col ,avg


# 1) Finding the 20 most common merchants in the US
hc = HiveContext(sc)
df = hc.table('business')

result = df.groupBy('name')\
    .agg(count('name').alias('cmt'))\
    .orderBy(col('cmt').desc())\
    .limit(20)
z.show(result)


# 2) Finding the top 10 cities with the most merchants in US
rslt = df.groupBy('city') \
    .agg(count('city').alias('merchant_count')) \
    .orderBy(col('merchant_count').desc()) \
    .limit(10)
z.show(rslt)


# 3)Analyze and list the top 10 cities with the highest average ratings.
avg_ratings= df.groupBy('city')\
            .agg(avg('stars').alias('avg_rating'))

top_10 = avg_ratings.orderBy(col('avg_rating').desc()).limit(10)
z.show(top_10)


#4. Find the most common merchants in the United States and display their average ratings (Top 20).

result = df.groupBy('name') \
           .agg(count('*').alias('count'), avg('stars').alias('avg_rating')) \
           .orderBy(col('count').desc()) \
           .limit(20)
z.show(result)


#5. Count the number of categories.


num_categories = df.select(countDistinct('categories').alias('Number_categories'))
num_categories.show()

#6. Identify and list the top 10 categories with the highest frequency


category_High_fr = df.groupBy('categories') \
                    .count() \
                    .orderBy(desc('count')) \
                    .limit(10)
z.show(category_High_fr)

#7. List the top 20 merchants with the most five-star reviews.


five_star_merchants = df.filter(col('stars') == 5)

top_20 = five_star_merchants.groupBy('name').count()\
            .orderBy(col('count').desc())\
            .limit(20)
top_20.show()


#8.Summarize the types and quantities of restaurants for different cuisines

american_summary = american_summary.withColumn('cuisine', lit('American')).limit(30)
mexican_summary = mexican_summary.withColumn('cuisine', lit('Mexican')).limit(30)
chinese_summary = chinese_summary.withColumn('cuisine', lit('Chinese')).limit(30)

american_summary = american_restaurants.groupBy('categories').count()
mexican_summary = mexican_restaurants.groupBy('categories').count()
chinese_summary = chinese_restaurants.groupBy('categories').count()

z.show(american_summary)
z.show(mexican_summary)
z.show(chinese_summary)
