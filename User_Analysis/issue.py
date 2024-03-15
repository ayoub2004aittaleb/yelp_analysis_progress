# Import necessary libraries
from pyspark import HiveContext
from pyspark.sql.functions import year, count, sum, when, col, to_date, desc

# Initialize HiveContext
hc = HiveContext(sc)

# Load data from the 'users' table
df = hc.table('users')

# Print the schema of the data
df.printSchema()

# 1. Analyze the yearly growth of user sign-ups.
df = df.withColumn("signup_date", to_date("user_yelping_since", "yyyy-MM-dd"))
user_signups = df.withColumn("signup_year", year("signup_date"))
yearly_signups = user_signups.groupBy("signup_year").count().orderBy("signup_year")
z.show(yearly_signups)

# 2. Count the "review_count" for users
total_review_count = df.agg(sum("user_review_count").alias("total_review_count"))
total_review_count.show()

# 3. Identify and list the most popular users based on their number of fans
most_popular_users = df.select('user_id', 'user_name', 'user_fans').orderBy(desc('user_fans'))
z.show(most_popular_users)

# 4. Display the yearly proportions of total users and silent users (those who haven't written reviews)
df = df.withColumn('silent_user', when(df.user_review_count == 0, 1).otherwise(0))
df = df.withColumn('registration_year', year(to_date(df.user_yelping_since, 'yyyy-MM-dd')))

user_counts_by_year = df.groupby('registration_year') \
                        .agg(count('*').alias('total_users'),
                             sum('silent_user').alias('silent_users'))

user_proportions_by_year = user_counts_by_year.withColumn('proportion_silent_users', 
                                                          user_counts_by_year.silent_users / user_counts_by_year.total_users) \
                                              .select('registration_year', 'total_users', 'proportion_silent_users')

user_proportions_by_year_ordered = user_proportions_by_year.orderBy(user_proportions_by_year.registration_year.desc())
z.show(user_proportions_by_year_ordered)

# 5. Summarize the yearly statistics for new users, review counts, elite users, total fans
df = df.withColumn('is_elite', when(df.user_elite == 'Yes', 1).otherwise(0))
df = df.withColumn('registration_year', year(df.user_yelping_since))

yearly_statistics = df.groupBy('registration_year').agg(count('*').alias('new_users'),
                           sum('user_review_count').alias('total_review_count'),
                           sum('is_elite').alias('elite_users'),
                           sum('user_fans').alias('total_fans')) \
                      .orderBy('registration_year')

z.show(yearly_statistics)
