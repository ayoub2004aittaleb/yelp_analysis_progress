# fint the top 10 ....

from pyspark import HiveContext
from pyspark.sql.functions import count, col

hc = HiveContext(sc)
df = hc.table('business')

result = df.groupBy('name')\
    .agg(count('name').alias('cmt'))\
    .orderBy(col('cmt').desc())\
    .limit(20)

z.show(result)

