# Databricks notebook source exported at Mon, 25 Jul 2016 04:48:19 UTC
# MAGIC %md
# MAGIC ### 1- Loading the log file from S3

# COMMAND ----------

ACCESS_KEY = "*"
SECRET_KEY = "*"
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "*"
MOUNT_NAME = "*"

dbutils.fs.unmount("/mnt/%s" % MOUNT_NAME)
dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

log_file_path = "/mnt/%s/2015_07_22_mktplace_shop_web_log_sample.log" % MOUNT_NAME
base_df = sqlContext.read.text(log_file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2- Parsing the log file

# COMMAND ----------

from pyspark.sql.functions import regexp_extract

# 2015-07-22T09:02:24.432545Z marketpalce-shop 203.115.101.197:14413 10.0.6.99:80 0.000022 0.095994 0.000019 200 200 0 1574 "GET https://paytm.com:443/shop/cart?channel=web&version=2 HTTP/1.1" "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2

split_df = base_df.select(regexp_extract('value', r'^([^\s]*).*', 1).cast('timestamp').alias('timestamp'),
                          #regexp_extract('value', r'^([^\s]*).*', 1).cast('timestamp').cast('long').alias('timestamp_long'),
                          #regexp_extract('value', r'^(?:[^\s]*\s)([^\s]*).*', 1).alias('elb'),
                          regexp_extract('value', r'^(?:[^\s]*\s){2}([^\s]*):.*', 1).alias('client_ip'),
                          #regexp_extract('value', r'^(?:[^\s]*\s){2}[^\s]*:(\d*).*', 1).alias('client_port'),
                          regexp_extract('value', r'^(?:[^\s]*\s){3}([^\s]*).*', 1).alias('backend_address'),
                          #regexp_extract('value', r'^(?:[^\s]*\s){4}([^\s]*).*', 1).alias('request_processing_time'),
                          #regexp_extract('value', r'^(?:[^\s]*\s){5}([^\s]*).*', 1).alias('backend_processing_time'),
                          #regexp_extract('value', r'^(?:[^\s]*\s){6}([^\s]*).*', 1).alias('response_processing_time'),
                          regexp_extract('value', r'^(?:[^\s]*\s){7}([^\s]*).*', 1).alias('elb_status_code'),
                          regexp_extract('value', r'^(?:[^\s]*\s){8}([^\s]*).*', 1).alias('backend_status_code'),
                          #regexp_extract('value', r'^(?:[^\s]*\s){9}([^\s]*).*', 1).alias('received_bytes'),
                          #regexp_extract('value', r'^(?:[^\s]*\s){10}([^\s]*).*', 1).alias('sent_bytes'),
                          #regexp_extract('value', r'^[^"]*\s"([^"]*)".*', 1).alias('request'),
                          regexp_extract('value', r'^[^"]*\s"[^"]*\s([^"]*)\s[^"]*".*', 1).alias('request_address'),
                          regexp_extract('value', r'^.*\s"(.*)".*', 1).alias('user_agent'),
                          #regexp_extract('value', r'^.*"\s(.*)\s.*', 1).alias('ssl_cipher'),
                          #regexp_extract('value', r'^.*\s(.*)$', 1).alias('ssl_protocol')
                         )

split_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3- Data Exploration

# COMMAND ----------

# MAGIC %md
# MAGIC - Noticed there are 164 records with '-' `backend_address`:  
# MAGIC `split_df.groupby('backend_address').count().collect()`  
# MAGIC   
# MAGIC - Turned out `backend_address` will be '-', if there is a 504 (Gateway timeout) error.
# MAGIC `split_df.filter(split_df['backend_address'] == '-').groupBy(['elb_status_code','backend_status_code']).count().collect()`
# MAGIC 
# MAGIC - Approximatly %7.6 of the traffic is coming from 3 IP addresses  
# MAGIC `(split_df.groupby('client_ip', 'user_agent').count().sort('count', ascending = False)).show(10, False)`    
# MAGIC  - 52.74.219.71 : 40633 - This is Google bot since the `user_agent` has google bot keyword  
# MAGIC  - 119.81.61.166 : 32829 - Not clear what is this IP
# MAGIC  - 106.186.23.95 : 14565 - Seems to be a service for checking promotions since it usually calls the `promotion` API: `cleanedup_sessionized_df.filter(cleanedup_sessionized_df['client_ip'] == '106.186.23.95').show(100, truncate = False)`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4- Processing and analytical goals

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, when, sum, concat, avg, countDistinct, lit
import sys

# Setting the threshold to 15 minutes
inactivity_threshold = 15 * 60

window = Window.partitionBy('client_ip').orderBy('timestamp')

# Adding a new column to check the inactivity time between the current row and the previous row (rows are ordered by timestamp)
seconds_since_last_activity = split_df['timestamp'].cast('long') - lag(split_df['timestamp'].cast('long'), default = 0).over(window)

# Adding a new column to check if the inactivity time is greater than the threshold
is_new_session = when(seconds_since_last_activity > inactivity_threshold, 1).otherwise(0)

# Building the session_id
session_id = \
  concat(split_df['client_ip'], \
         lit('::'), \
         (sum(is_new_session).over(window.rowsBetween(-sys.maxsize, 0))).cast('string'))

sessionized_df = \
  split_df \
    .select('*', \
            seconds_since_last_activity.alias('seconds_since_last_activity'), \
            is_new_session.alias('is_new_session'), \
            session_id.alias('session_id'))

last_activity = when(sessionized_df['is_new_session'] == 1, 0).otherwise(sessionized_df['seconds_since_last_activity'])

# Sessionizing the records
cleanedup_sessionized_df = sessionized_df \
                            .select('*',
                                    last_activity.alias('last_activity'))

# COMMAND ----------

# Create a new data frame with session id and session time
session_time_df = cleanedup_sessionized_df.select('*').groupBy('session_id').agg(sum('last_activity').alias('session_time'))

# COMMAND ----------

# Average session time
average_session_time = session_time_df.select(avg(session_time_df['session_time'])).collect()
print 'Average Session Time (seconds): %.4f' % average_session_time[0][0]

# COMMAND ----------

# Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
cleanedup_sessionized_df \
  .select('*') \
  .groupBy('session_id') \
  .agg(cleanedup_sessionized_df['session_id'], countDistinct(cleanedup_sessionized_df['request_address'])) \
  .show(20, truncate = False)

# COMMAND ----------

# Most engaged users
session_time_df.select('*').orderBy('session_time', ascending = False).collect()

# COMMAND ----------


