from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta import *
from delta.tables import DeltaTable
from email_notify import send_mail_alert
from delta.pip_utils import configure_spark_with_delta_pip
import uuid
import sys


# In[2]:


# spark = SparkSession.builder.appName("AdvertiseX").getOrCreate()
spark = (
    SparkSession
    .builder#.master("spark://spark:7077")
    .appName("AdvertiseX")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(spark).getOrCreate()
spark.conf.set('spark.sql.streaming.statefulOperator.allowMultiple','false')
# spark.conf.set('spark.sql.repl.eagerEval.enabled', True)


# In[6]:


# Data ingestion definition
def ingest_data(schema_map,ip_path_map):
    impressions_path = ip_path_map['impressions_path']
    clicks_path = ip_path_map['clicks_path']
    bid_requests_path = ip_path_map['bid_requests_path']
    if stream_mode == 'batch':
        impressions_df = spark.read.option("multiline","true").json(impressions_path, schema=schema_map['impression_schema'])
        clicks_df = spark.read.csv(clicks_path, schema=schema_map['click_conversion_schema'],header=True)
        bid_requests_df = spark.read.csv(bid_requests_path, schema=schema_map['bid_request_schema'],header=True)
        
        return impressions_df, clicks_df, bid_requests_df
    
    elif stream_mode == 'streaming':
        impressions_df = spark.readStream.option("multiline","true").json(impressions_path, schema=schema_map['impression_schema'])
        clicks_df = spark.readStream.csv(clicks_path, schema=schema_map['click_conversion_schema'],header=True)
        bid_requests_df = spark.readStream.csv(bid_requests_path, schema=schema_map['bid_request_schema'],header=True)

        
        return impressions_df, clicks_df, bid_requests_df
    
    else:
        raise ValueError("Invalid mode. Use 'batch' or 'streaming'.")

#Source Data Validation

# Define a function for validation and email alerting
def validate_data(df, schema, schema_name):
    """
    Validates DataFrame against the provided schema.
    Sends email alert if validation fails.
    """
    errors = []
    
    # Check for missing columns
    missing_cols = [field.name for field in schema.fields if field.name not in df.columns]
    if missing_cols:
        errors.append(f"Missing columns in {schema_name} schema: {missing_cols}")
    
    # Check for data type mismatches
    for field in schema.fields:
        if field.name in df.columns:
            actual_type = df.schema[field.name].dataType
            expected_type = field.dataType
            if not isinstance(actual_type, expected_type.__class__):
                errors.append(f"Data type mismatch for column {field.name} in {schema_name} schema. Expected {expected_type}, but found {actual_type}")
    
    # Check for null values in each column
    null_counts = None
    if df.isStreaming:
        # Stream case: Trigger a micro-batch query for null value counts
    
        # Ensure appropriate checkpointing for micro-batch queries
        # query_name = f"null_value_check_{schema_name.replace(' ', '_')}" 
        query_name = f"null_value_check_{uuid.uuid1().hex}"
        query = df.writeStream \
            .format("memory") \
            .option("checkpointLocation", checkpoint_location+uuid.uuid1().hex) \
            .trigger(processingTime="10 second") \
            .queryName(query_name) \
            .start()
    
        # Execute the query and collect the results, handling potential errors
        try:
            query.awaitTermination(10)  # Wait for the query to finish processing
        except Exception as e:
            error_msg = f"Error during micro-batch query for null value check in schema '{schema_name}': {e}"
            errors.append(error_msg)
        else:
            recent_progress_df = spark.sql(f"SELECT * FROM {query_name}")
            null_counts = recent_progress_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()

    else:
        null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()

    if null_counts:
        null_columns = {col: count for col, count in null_counts.items() if count > 0}
        if null_columns:
            null_columns_str = "\n".join([f"\t{col}: {count}" for col, count in null_columns.items()])
            errors.append(f"Null values found in {schema_name} schema:\n{null_columns_str}")
    
    # Send email alert if errors exist
    if errors:
        error_message = f"Validation failed for {schema_name} schema. Errors found:\n" + "\n".join(errors)
        print('Validation error :\n',error_message)
        #send_mail_alert(subject=f"Validation Failed: {schema_name} Schema", message_body=error_message)
    else:
        print(f"Validation successful for {schema_name} schema.")


# Data tranformation logic

def process_data(impressions_df,bid_requests_df,clicks_df,out_path_map):
    ### Handling missing values ###
    impressions_df = impressions_df.fillna('Unknown', 'geo_location')
    bid_requests_df = bid_requests_df.fillna('NA', 'browser')
    clicks_df = clicks_df.dropna(subset=['conversion_type'])
    
    if bid_requests_df.isStreaming:
        query_name = f"mean_bid_price_{uuid.uuid1().hex}"
        query = bid_requests_df.writeStream \
            .format("memory") \
            .option("checkpointLocation", checkpoint_location+uuid.uuid1().hex) \
            .trigger(processingTime="10 second") \
            .queryName(query_name) \
            .start()
        try:
            query.awaitTermination(10) 
        except Exception as e:
            print("Error during micro-batch query for mean bid price")
        else:
            recent_progress_df = spark.sql(f"SELECT * FROM {query_name}")
            mean_bid_price = round(recent_progress_df.select(F.mean('bid_price')).collect()[0][0],2)

    else:
        mean_bid_price = round(bid_requests_df.select(F.mean('bid_price')).collect()[0][0],2)
    
    bid_requests_df = bid_requests_df.fillna(mean_bid_price, subset=['bid_price'])
    
    ### Standardizing Timestamps ###
    impressions_df = impressions_df.withColumn(
        'ad_impressions_ts', F.to_timestamp('timestamp', 'yyyy-MM-dd HH:mm:ss EST')).drop('timestamp')
    clicks_df = clicks_df.withColumn(
        'conversions_ts_est', F.to_timestamp('event_timestamp', 'yyyy-MM-dd HH:mm:ss EST')).drop('event_timestamp')
    bid_requests_df = bid_requests_df.withColumn(
        'bid_req_ts_est', F.to_timestamp('event_timestamp', 'yyyy-MM-dd HH:mm:ss EST')).drop('event_timestamp')

    ### Feature Extraction ###
    
    # Time-based Features
    impressions_df = impressions_df.withColumn('hour_of_day', F.hour(F.col('ad_impressions_ts')))\
                                   .withColumn('day_of_week', F.dayofweek(F.col('ad_impressions_ts')))\
                                   .withColumn('month', F.month(F.col('ad_impressions_ts')))\
                                   .withColumn('year', F.year(F.col('ad_impressions_ts')))
    
    
    # Geolocation Analysis
    impressions_df = impressions_df.withColumn('country', F.split(F.col('geo_location'), '-').getItem(0))\
                                   .withColumn('region', F.split(F.col('geo_location'), '-').getItem(1))\
                                   .withColumn('city', F.split(F.col('geo_location'), '-').getItem(2))
    
    bid_requests_df_with_wm = bid_requests_df.withWatermark("bid_req_ts_est", "10 minutes")
    avg_spent_df = bid_requests_df_with_wm\
    .groupBy(F.window("bid_req_ts_est", "10 minutes"),'user_id').agg(
        F.mean('bid_price').alias('avg_spent')
    ).withColumn('avg_spent_threshold',
        F.when(F.col('avg_spent') < 0.4, F.lit('low'))
        .when(F.col('avg_spent') >= 0.7, F.lit('high'))
        .otherwise(F.lit('moderate'))
    ).withColumn('updated_ts',F.current_timestamp())\
    .orderBy(F.desc("avg_spent"))
    
    ### Data Enrichment and Transformation ###
    #Joining Dataframes
    
    impressions_df_with_wm = impressions_df.withWatermark("ad_impressions_ts", "10 minutes")
    clicks_df_with_wm = clicks_df.withWatermark("conversions_ts_est", "5 minutes")
    
    joined_df = impressions_df_with_wm.alias("impressions_df").join(
    clicks_df_with_wm.alias("clicks_df").drop("ad_campaign_id", "user_id"),
    F.expr("impressions_df.bid_req_id = clicks_df.bid_req_id AND ad_impressions_ts >= conversions_ts_est - interval 2 minutes"),
    "leftOuter"
                ).select("impressions_df.*", "clicks_df.conversion_type")
    
    bid_requests_df_with_wm_1 = bid_requests_df.withWatermark("bid_req_ts_est", "10 minutes")
    joined_df2 = joined_df.alias("joined_df").join(
    bid_requests_df_with_wm_1.alias("bid_requests_df").drop("ad_campaign_id", "user_id"),
    F.expr("joined_df.bid_req_id = bid_requests_df.bid_req_id \
    AND joined_df.ad_impressions_ts >= bid_req_ts_est - interval 2 minutes "),
    "leftOuter"
                )
    
    # User Data extraction
    user_df = joined_df2.select('user_id','geo_location','user_age','user_gender','user_interests')\
    .withColumn('updated_ts',F.current_timestamp())

    joined_df_type_1 = joined_df2.select('conversion_type','user_id','ad_campaign_id','conversion_type','bid_req_ts_est')
    funnel_df = joined_df_type_1 \
    .filter(~F.col('conversion_type').isNull()) \
    .withWatermark("bid_req_ts_est", "10 minutes") \
    .groupBy(F.window("bid_req_ts_est", "10 minutes"), 'user_id') \
    .agg(F.collect_list('ad_campaign_id').alias('ad_campaign_list'),
         F.collect_list('conversion_type').alias('conversion_type_list')) \
    .withColumn('updated_ts', F.current_timestamp())

    
    #Aggregation

    joined_df_type_2 = joined_df2.select('user_id','ad_campaign_id','conversion_type','bid_req_ts_est')
    campaign_ctr_df = joined_df_type_2 \
    .withWatermark("bid_req_ts_est", "10 minutes") \
    .groupBy(F.window("bid_req_ts_est", "10 minutes"),'ad_campaign_id') \
    .agg(F.approx_count_distinct('user_id').alias('impressions'), F.count('conversion_type').alias('conversions')) \
    .withColumn('ctr', F.round(F.col('conversions') / F.col('impressions'), 3)) \
    .withColumn('updated_ts', F.current_timestamp())

    joined_df_type_3 = joined_df2.select('website','conversion_type','user_id','bid_req_ts_est')
    website_conversion_rate_df = joined_df_type_3 \
    .withWatermark("bid_req_ts_est", "10 minutes") \
    .groupBy(F.window("bid_req_ts_est", "10 minutes"),'website', 'conversion_type') \
    .agg(F.count('conversion_type').alias('conversions'), F.approx_count_distinct('user_id').alias('unique_users')) \
    .withColumn('conversion_rate', F.round(F.col('conversions') / F.col('unique_users'), 3)) \
    .withColumn('updated_ts', F.current_timestamp())

    
    #Grouping

    joined_df_type_4 =  joined_df2.select('user_id','conversion_type','bid_req_ts_est')
    user_performance_df = joined_df_type_4 \
    .withWatermark("bid_req_ts_est", "10 minutes") \
    .groupBy(F.window("bid_req_ts_est", "10 minutes"),'user_id') \
    .agg(F.approx_count_distinct('user_id').alias('impressions'), F.count('conversion_type').alias('conversions')) \
    .withColumn('ctr', F.round(F.col('conversions') / F.col('impressions'), 3)) \
    .withColumn('updated_ts', F.current_timestamp())


    if stream_mode == 'streaming':
    # Data write function call
        query_avg_spent = write_data(avg_spent_df,out_path_map['avg_spent_path'],stream_mode,'complete','avg_spent_threshold')
        query_user = write_data(user_df,out_path_map['user_path'],stream_mode,'append','user_age')
        query_funnel = write_data(funnel_df,out_path_map['funnel_path'],stream_mode,'append')
        query_campaign_ctr = write_data(campaign_ctr_df,out_path_map['campaign_ctr_path'],stream_mode,'append')
        query_website_conversion = write_data(website_conversion_rate_df,out_path_map['website_conversion_rate_path'],stream_mode,'append','website')
        query_user_performance = write_data(user_performance_df,out_path_map['user_performance_path'],stream_mode,'append')
    
        if query_avg_spent:
            query_avg_spent.awaitTermination()
        if query_user:
            query_user.awaitTermination()
        if query_funnel:
            query_funnel.awaitTermination()
        if query_campaign_ctr:
            query_campaign_ctr.awaitTermination()
        if query_website_conversion:
            query_website_conversion.awaitTermination()
        if query_user_performance:
            query_user_performance.awaitTermination()
            
    elif stream_mode == 'batch':
        write_data(avg_spent_df,out_path_map['avg_spent_path'],stream_mode,'append','avg_spent_threshold')
        write_data(user_df,out_path_map['user_path'],stream_mode,'append','user_age')
        write_data(funnel_df,out_path_map['funnel_path'],stream_mode,'append')
        write_data(campaign_ctr_df,out_path_map['campaign_ctr_path'],stream_mode,'append')
        write_data(website_conversion_rate_df,out_path_map['website_conversion_rate_path'],stream_mode,'append','website')
        write_data(user_performance_df,out_path_map['user_performance_path'],stream_mode,'append')
    

def write_data(df,path,stream_mode,write_mode,partition_column=None):
    if stream_mode == 'batch':
        if partition_column:
            df.write.partitionBy(partition_column).format("delta").mode(write_mode).save(path)
        else:
            df.write.format("delta").mode(write_mode).save(path)
        return None
    elif stream_mode == 'streaming':
        if partition_column:
            streaming_query = df \
                .writeStream \
                .partitionBy(partition_column) \
                .format("delta") \
                .option("checkpointLocation", checkpoint_location+uuid.uuid1().hex) \
                .outputMode(write_mode) \
                .start(path)
        else:
            streaming_query = df \
                .writeStream \
                .format("delta") \
                .option("checkpointLocation", checkpoint_location+uuid.uuid1().hex) \
                .outputMode(write_mode) \
                .start(path)
    
        return streaming_query


def main(impressions_path,
        clicks_path,
        bid_requests_path,
        website_conversion_rate_path,
		avg_spent_path,
		user_path,
		funnel_path,
		campaign_ctr_path,
		user_performance_path,
		stream_mode):
    # Schema 
    impression_schema = StructType([
        StructField("ad_creative_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("website", StringType(), True),
        StructField("ad_format", StringType(), True),
        StructField("geo_location", StringType(), True),
        StructField("ad_campaign_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("bid_req_id", StringType(), True)
    ])
    
    click_conversion_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("ad_campaign_id", StringType(), True),
        StructField("conversion_type", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("bid_req_id", StringType(), True)
    ])
    
    bid_request_schema = StructType([
        StructField("bid_req_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_age", IntegerType(), True),
        StructField("user_gender", StringType(), True),
        StructField("user_interests", StringType(), True),
        StructField("user_location", StringType(), True),
        StructField("bid_price", DoubleType(), True),
        StructField("device_type", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("application", StringType(), True),
        StructField("ad_campaign_id", StringType(), True),
        StructField("event_timestamp", TimestampType(), True)
    ])
    
    
    schema_map = {
        'impression_schema':impression_schema,
        'click_conversion_schema':click_conversion_schema,
        'bid_request_schema':bid_request_schema
    }
    
    ip_path_map = {
        'impressions_path':impressions_path,
        'clicks_path':clicks_path,
        'bid_requests_path':bid_requests_path
    }
    
    out_path_map = {
        'website_conversion_rate_path':website_conversion_rate_path,
        'avg_spent_path':avg_spent_path,
        'user_path':user_path,
    	'funnel_path':funnel_path,
    	'campaign_ctr_path':campaign_ctr_path,
    	'user_performance_path':user_performance_path
    }
    
    
    # Data ingestion function call
    impressions_df,clicks_df,bid_requests_df = ingest_data(schema_map,ip_path_map)
    
    #Data Validation
    validate_data(impressions_df, impression_schema, 'Ad Impressions')
    validate_data(clicks_df, click_conversion_schema, 'Clicks and Conversions')
    validate_data(bid_requests_df, bid_request_schema, 'Bidding Request')
    
    # Data process function call
    process_data(impressions_df,bid_requests_df,clicks_df,out_path_map)

if __name__ == '__main__':
    global checkpoint_location,stream_mode
    args = sys.argv[1:]  
    impressions_path = args[args.index("--impressions_path") + 1]
    clicks_path = args[args.index("--clicks_path") + 1]
    bid_requests_path = args[args.index("--bid_requests_path") + 1]
    website_conversion_rate_path = args[args.index("--website_conversion_rate_path") + 1]
    avg_spent_path = args[args.index("--avg_spent_path") + 1]
    user_path = args[args.index("--user_path") + 1]
    funnel_path = args[args.index("--funnel_path") + 1]
    campaign_ctr_path = args[args.index("--campaign_ctr_path") + 1]
    user_performance_path = args[args.index("--user_performance_path") + 1]
    stream_mode = args[args.index("--stream_mode") + 1]
    checkpoint_location = args[args.index("--checkpoint_location") + 1]

    main(impressions_path,
        clicks_path,
        bid_requests_path,
        website_conversion_rate_path,
        avg_spent_path,
        user_path,
        funnel_path,
        campaign_ctr_path,
        user_performance_path,
        stream_mode)
