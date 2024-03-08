# 1) Finding the 20 most common merchants in the US

from pyspark import HiveContext
from pyspark.sql.functions import count, col

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
