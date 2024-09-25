from pyspark.sql.window import Window
from pyspark.sql import Window,SparkSession
from pyspark.sql.functions import when, col, concat_ws, upper, max, md5, rank, row_number, collect_list, sort_array
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, lit
S3_INPUT_DATA_PATH  = 's3://ascend-oeidp-batch-pinning-input-074628058490-us-east-1/AtData/supertrace_atdata/atdata_rawfiles/experian_ad_fullbuild_202407_01.txt.gz'
S3_OUTPUT_DATA_PATH = ''
def main():
    spark                   = SparkSession.builder.appName("atdata_pre_process").getOrCreate()
    
    df_limit                =   spark.read.format("csv").\
                                option("delimiter", "\t").\
                                option("inferSchema", "false").\
                                option("header", "false").load(S3_INPUT_DATA_PATH)

    new_fieldnames_fields   = ["email", "first_name", "last_name", "address1", "address2", "city", "state", "zip5", "zip4", "website", "ip", "opt_in_date","email_status_id"]

    df                      = df_limit.toDF(*new_fieldnames_fields)
    df                      = df.dropna(how = 'email')
    valid_df                = df.filter("email_status_id = 1")
    hash_cols               =  ['first_name', 'last_name', 'address1', 'city', 'state', 'zip5', 'zip4']

    for hash_col in hash_cols:
        valid_df = valid_df.withColumn(hash_col, when(col(hash_col).isNull(), "").otherwise(col(hash_col)))

    valid_df    = valid_df.withColumn("concat", upper(concat_ws('\t', *hash_cols))).withColumn('hashkey', md5('concat')).withColumn("full_zip",concat_ws(col("zip5"), col("zip4")))

    valid_df    = valid_df.drop("concat")

    windowSpec  = Window.partitionBy(lit("1")).orderBy(
                    lit("1")    
                    )
    df = valid_df.withColumn("record_sequence_no", row_number().over(windowSpec)).withColumn("record_seq_no", col("record_sequence_no")).withColumn("full_zip",concat_ws(col("zip5"), col("zip4")))


if __name__ == '__main__':
    main()
