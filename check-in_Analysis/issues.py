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

#--------------------------------------------------------------------------

from pyspark.sql.functions import hour

# Create a SparkSession
spark = SparkSession.builder \
    .appName("HourlyCheckinFrequency") \
    .master('local') \
    .getOrCreate()

# Define the schema
fire_schema = StructType([
    StructField('business_id', StringType(), True),
    StructField('date', TimestampType(), True)
])

# Read JSON data with the defined schema
df = spark.read.json('data/yelp_academic_dataset_checkin.json', schema=fire_schema)

# Filter out null values
df = df.filter(df['business_id'].isNotNull() & df['date'].isNotNull())

# Extract the hour from the 'date' column
df = df.withColumn("hour", hour(df["date"]))

# Group the data by hour and count the number of check-ins for each hour
hourly_checkin_frequency = df.groupBy("hour").count().orderBy("hour")

# Show the result
hourly_checkin_frequency.show(24)  # Showing all 24 hours

#--------------------------------------------------------------------------

# Create a SparkSession
spark = SparkSession.builder \
    .appName("MostFrequentCheckinBusinesses") \
    .master('local') \
    .getOrCreate()

# Read JSON data
df = spark.read.json('data/yelp_academic_dataset_checkin.json')

# Filter out null values
df = df.filter(df['business_id'].isNotNull() & df['date'].isNotNull())

# Split the date string into an array of dates, then explode the array into separate rows
df_exploded = df.withColumn("date", explode(split(df["date"], ", ")))

# Now, group by business_id and count the number of check-ins (now that they're separated)
business_checkin_frequency = df_exploded.groupBy("business_id").count().orderBy(desc("count"))

# Show the result
business_checkin_frequency.show()
