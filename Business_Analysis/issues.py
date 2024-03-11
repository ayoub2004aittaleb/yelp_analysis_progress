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