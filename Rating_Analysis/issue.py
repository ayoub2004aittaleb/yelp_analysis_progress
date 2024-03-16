from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower
from pyspark.ml.feature import StopWordsRemover
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, dayofweek  # Import the col function and the day of week function

spark = SparkSession.builder.appName("Rating").getOrCreate()


df = spark.read.json("/content/yelp_academic_dataset_tip.json")

words = df.select(explode(split(lower(df.text), "\\s+")).alias("word"))

# Remove common stop words, if necessary
# You might need to adjust the list of stop words based on your requirements
stop_words = StopWordsRemover().getStopWords()
words = words.filter(~words.word.isin(stop_words))

# Count the occurrence of each word
word_counts = words.groupBy("word").count()

# Sort the words by their counts in descending order
top_20_words = word_counts.orderBy("count", ascending=False).limit(20)

# Show the top 20 common words
top_20_words.show()





words = df.select(explode(split(lower(df.text), "\\s+")).alias("word"))

# Remove common stop words, if necessary
# You might need to adjust the list of stop words based on your requirements
stop_words = StopWordsRemover().getStopWords()
words = words.filter(~words.word.isin(stop_words))

# Count the occurrence of each word
word_counts = words.groupBy("word").count()

# Sort the words by their counts in descending order
top_20_words = word_counts.orderBy("count", ascending=False).limit(20)

# Collect data from DataFrame to local Python environment
top_20_words_data = top_20_words.collect()

# Extract words and counts for plotting
words_list = [row["word"] for row in top_20_words_data]
counts_list = [row["count"] for row in top_20_words_data]

# Plot the bar graph
plt.figure(figsize=(10, 6))
plt.bar(words_list, counts_list, color='skyblue')
plt.xlabel('Words')
plt.ylabel('Frequency')
plt.title('Top 20 Common Words in Reviews')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()


df = spark.read.json("/content/yelp_academic_dataset_business.json")
df.show()
stars_column = df.select(col("stars"))

# Group by the 'stars' column and count the occurrences of each rating
rating_distribution = stars_column.groupBy("stars").count().orderBy("stars")

# Show the distribution of ratings
rating_distribution.show()



# Group by the 'stars' column and count the occurrences of each rating
rating_distribution = stars_column.groupBy("stars").count().orderBy("stars")

# Collect data from DataFrame to local Python environment
rating_distribution_data = rating_distribution.collect()

# Extract ratings and counts for plotting
ratings_list = [row["stars"] for row in rating_distribution_data]
counts_list = [row["count"] for row in rating_distribution_data]

# Plot the bar graph with cleaner styling
plt.figure(figsize=(8, 6))
plt.bar(ratings_list, counts_list, color='skyblue', edgecolor='black')
plt.xlabel('Rating', fontsize=14)
plt.ylabel('Frequency', fontsize=14)
plt.title('Distribution of Ratings', fontsize=16)
plt.xticks(ratings_list)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()



five_star_businesses = df.filter(df.stars == 5.0)

# Group by business name and count the occurrences
top_5_merchants = five_star_businesses.groupBy("name").count().orderBy(col("count").desc()).limit(5)

# Show the top 5 merchants with the most 5-star ratings
top_5_merchants.show(truncate=False)


# Group by business name and count the occurrences
top_5_merchants = five_star_businesses.groupBy("name").count().orderBy(col("count").desc()).limit(5)

# Collect data from DataFrame to local Python environment
top_5_merchants_data = top_5_merchants.collect()
merchant_names = [row["name"] for row in top_5_merchants_data]
ratings_counts = [row["count"] for row in top_5_merchants_data]

# Plot the bar graph
plt.figure(figsize=(10, 6))
plt.barh(merchant_names, ratings_counts, color='skyblue')
plt.xlabel('Number of 5-star Ratings', fontsize=14)
plt.ylabel('Merchant Name', fontsize=14)
plt.title('Top 5 Merchants with the Most 5-star Ratings', fontsize=16)
plt.gca().invert_yaxis()  # Invert y-axis to display the merchant with the highest rating on top
plt.grid(axis='x', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()