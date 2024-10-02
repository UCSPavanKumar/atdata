from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, row_number, rank, count, max as F_max, to_json, struct, from_json, collect_list, expr, explode
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import * #when, col, concat_ws, upper, max, md5, rank, row_number, collect_list, sort_array, like, contains,count,sha2,concat_ws,lit,monotonically_increasing_id
import pyspark as spark
import json
from pyspark.sql import SparkSession
import sys
import argparse
import boto3
def main(S3_PINNED_INPUT_PATH,S3_RAW_CLIENT_DATA_PATH,S3_RESULT_OUTPUT_PATH):
# Initialize Spark session
    spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

    # Read data
    df1 = spark.read.parquet(S3_PINNED_INPUT_PATH )
    df2 = spark.read.option("header", "true").option("delimiter", "|").option("encoding", "UTF-8").csv(S3_RAW_CLIENT_DATA_PATH)
    #f2 = df2.withColumn("opt_in_date", date_format(to_timestamp(col("opt_in_date"), "yyyy-MM-dd'T'HH:mm:ss'Z'"), "yyyy/MM/dd"))

    # Select and join data
    df1 = df1.select(col('eid'), col('hashkey'))
    valid_df = df1.join(df2, ['hashkey']).dropna(subset=['email'])

    # Group and aggregate data
    grouped_df = valid_df.groupBy("eid", "email", "opt_in_date").agg(count("record_seq_no").alias("eid_email_freq"))
    valid_df = valid_df.join(grouped_df, ["eid", "email", "opt_in_date"])

    # Add is_gmail column
    valid_df = valid_df.withColumn("is_gmail", when(col("email").like("%gmail.com%"), 1).otherwise(0))

    # Define window specifications
    windowSpec = Window.partitionBy("eid").orderBy(
        col("opt_in_date").desc(),
        col("is_gmail").desc(),
        col("eid_email_freq").desc(),
        col("email").asc()
        )

    # Add email_rank and rank columns
    valid_df = valid_df.withColumn("email_rank", row_number().over(windowSpec))
    window = Window.partitionBy(valid_df['eid']).orderBy(valid_df['email_rank'].desc())
    valid_df = valid_df.select('*', rank().over(window).alias('rank'))

    # Further window specifications and ranking
    #ranking duplicate rows of eid,email_freq,opt_int_date
    windowspec1 = Window.partitionBy(['eid', 'eid_email_freq', 'opt_in_date']).orderBy(col('rank'))
    x_df = valid_df.select('*', rank().over(windowspec1).alias('record_seq_rank'))
    #Extracting the highest rank from record_seq_rank
    y_df = x_df.groupBy(['eid', 'email', 'opt_in_date']).agg(F_max(col('record_seq_rank')).alias('record_seq_rank'))
    r#anking eid and email using opt_in_dat edfand extracting the recent emaiolipt in date I
    window_1 = Window.partitionBy(['eid', 'email']).orderBy(col('opt_in_date').desc())
    t_df = y_df.select('*', rank().over(window_1).alias('test_rank')).filter(col('test_rank') == 1)
    #assigning rank to dataframe using unique opt_in_date for ordering same eid unique email ids 
    window_2 = Window.partitionBy(['eid']).orderBy(col('opt_in_date').desc())
    t_df = t_df.select('*', rank().over(window_2).alias('rank'))

    # Convert to JSON and back to DataFrame
    t_df = t_df.withColumn("first_emails", to_json(struct("rank", "email", 'opt_in_date')))
    #creation of email schema
    email_schema = StructType([
        StructField("rank", IntegerType(), True),
        StructField("email", StringType(), True),
        StructField("opt_in_date", StringType(), True)
        ])
    #assigning email schema to emails column
    t_df = t_df.withColumn("first_emails", from_json(col("first_emails"), email_schema))

    # Group by eid and collect emails(list of struct arrays)
    t_df = t_df.groupBy('eid').agg(collect_list('first_emails').alias("emails"))
    
    # writing output to parquet file
    t_df.write.parquet(S3_RESULT_OUTPUT_PATH)
   
    
	#   RECORD_Partition_01,3500
	# TOTAL RECORDS,3500
	# AMOUNT TO PIN,10%
	# PINNING RATE,10%
	# TRADE DATE, 2024-09-17
	# Batch Identifier,1
if __name__ == '__main__':
				S3_PINNED_INPUT_PATH = sys.argv[1]
				S3_RAW_CLIENT_DATA_PATH = sys.argv[2]
				S3_RESULT_OUTPUT_PATH 		= sys.argv[3]
    main(S3_PINNED_INPUT_PATH,S3_RAW_CLIENT_DATA_PATH,S3_RESULT_OUTPUT_PATH)












