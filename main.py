import pyspark
from pyspark.sql import SparkSession
import re
import findspark

spark = SparkSession.builder.getOrCreate()

# loading 'country' table
country_df = spark.read.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/world",
    driver = "com.mysql.jdbc.Driver",
    dbtable = "country",
    user="root",
    password="2403")\
    .load()

# loading 'country language' table
countrylanguage_df = spark.read.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/world",
    driver = "com.mysql.jdbc.Driver",
    dbtable = "countrylanguage",
    user="root",
    password="2403")\
    .load()

# joining both the tables based on 'countary codes' column
new_df = country_df.join(countrylanguage_df, country_df.Code == countrylanguage_df.CountryCode, how='left')

# writing table back to Database 
new_df.write.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/world",
    driver = "com.mysql.jdbc.Driver",
    dbtable = "country_with_language",
    user="root",
    password="2403").save()

