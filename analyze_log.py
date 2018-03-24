import re
import sys
import datetime
from pyspark.sql.functions import split, regexp_extract, sum
from pyspark.sql import SparkSession

if len(sys.argv) != 3:
    print ('Usage: spark-submit ' + sys.argv[0] + ' <appName> <master_node>')
    sys.exit(1)

appName = sys.argv[1]
master = sys.argv[2]

# Setup SparkConf and SparkContext

try:
    ss = SparkSession.builder.appName(appName).master(master).getOrCreate()
    print ('Started app ' + appName + ' with master ' + master)
except:
    print ('Error creating SparkSession')
    sys.exit(1)

logfile = 'server_log'

logfileDF = ss.read.text(logfile)

# These regexps are from CS105x
parsedDF = logfileDF.select(regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'),
                          regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('timestamp'),
                          regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('path'),
                          regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('status'),
                          regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('content_size'))

#cache the dataframe
parsedDF.cache()

# Create a view from the dataframe. This will be useful for using regular SQL queries.
parsedDF.createOrReplaceTempView("parsedDF")

# Some statistics
print ("Number of entries in log file = " + str(parsedDF.count()))

print ("Number of successful requests (status 200) = " + str(parsedDF.filter('status=200').count()))

print ("Success ratio = " + str(float(parsedDF.filter('status=200').count())/parsedDF.count()*100) + "%")

# Find most accessed object with a SQL query
frequentDF = ss.sql('SELECT path, COUNT(path) FROM parsedDF GROUP BY path ORDER BY Count(path) DESC')

print 'The most accessed object is ' + str(frequentDF.collect()[0].path)

contentDF = parsedDF.sort('content_size', ascending = False)

print("The largest object accessed is " + str(contentDF.take(1)[0].path))

aggDF = contentDF.agg(sum('content_size').alias('total_content'))

total = aggDF.take(1)[0].total_content     # Using take instead of collect although there is only a single row (following standard practice of not using collect())

print ("Total traffic = " + str(total) + " bytes")

print ("The log was split into " +str(parsedDF.rdd.getNumPartitions()) + " partitions.")

# Clear dataframe from cache
parsedDF.unpersist()

''' Stop the SparkContext'''
ss.stop()

print ("Spark session closed.")
