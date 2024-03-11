from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import year, col

# Create a SparkSession
spark = SparkSession.builder \
    .appName("YearlyCheckinFrequency") \
    .master('local') \
    .getOrCreate()

# Define the schema
fire_schema = StructType([
    StructField('business_id', StringType(), True),
    StructField('date', TimestampType(), True)
])

# Read JSON data with the defined schema
df = spark.read.json('data/yelp_academic_dataset_checkin.json', schema=fire_schema)

df = df.filter(col('business_id').isNotNull() & col('date').isNotNull())
# Extract the year from the 'date' column
df = df.withColumn("year", year(df["date"]))

# Group the data by year and count the number of check-ins for each year
yearly_checkin_frequency = df.groupBy("year").count().orderBy("year")

# Show the result
yearly_checkin_frequency.show()

