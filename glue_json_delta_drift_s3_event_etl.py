
#----------------------Utility Functions----------------------------------------------#


# A helper function to explode the array elements present in the json
def explode_data(df):
    for field in df.schema.fields:
        if isinstance(field.dataType,ArrayType):
            df = df.withColumn(field.name,explode_outer(field.name))
    return df


# A helper recursive function to flatten the nested keys present in the json
def get_flattened_cols(df_schema_fields,flat_cols=[],prefix=""):
    for field in df_schema_fields:
        if isinstance(field.dataType,StructType):
            get_flattened_cols(field.dataType.fields,flat_cols,field.name)
        else:
            flat_col = [field.name if len(prefix)==0 else prefix + "." + field.name ]
            flat_cols.append(flat_col[0])
    return dict.fromkeys(flat_cols)


# A helper function to standardize the column names of the flatten json schema
def standardize_col_name(col_name):
    col_name = list(col_name)
    for ind,col_char in enumerate(col_name):
        if col_char == "." and ind+1 < len(col_name):
            col_name[ind+1] = col_name[ind+1].upper()
    col_name_derived = "".join(col_name)
    return  col_name_derived.replace(".","")


from pyspark.sql.functions import *

# Main driver function to flatten the input nested json into a denormalized tabular structure
def flatten_json(data):
    df = explode_data(data)
    df_cols = list(dict.fromkeys(get_flattened_cols(df.schema.fields,[])))
    df_cols_renamed = [col(df_col).alias(standardize_col_name(df_col)) for df_col in df_cols]
    return df.select(*[df_cols_renamed])


# A helper function for rearranging columns in order of the partition columns defined
def rearrange_schema(df,partition_cols):
    non_partition_cols = [col for col in df.columns if col not in partition_cols]
    ordered_cols = partition_cols + non_partition_cols
    return df.select(*ordered_cols)


import boto3

# A helper function that executes query in Athena
def execute_athena_query(query,database_name,results_loc):
    athena_client = boto3.client("athena")
    response = athena_client.start_query_execution(
    QueryString = query,
    QueryExecutionContext = {
        'Database': database_name
        }, 
    ResultConfiguration = { 'OutputLocation': results_loc}
    )
    return response
    

import boto3
import time

# A helper function that waits until the query in Athena has fully executed
def wait_for_query_execution(query_response):
    athena_client = boto3.client("athena")
    queryExecution = athena_client.get_query_execution(QueryExecutionId=query_response['QueryExecutionId'])
    query_status = queryExecution["QueryExecution"]["Status"]["State"]
    counter = 0
    while (query_status != "SUCCEEDED" and counter < 9):
        time.sleep(2)
        queryExecution =athena_client.get_query_execution(QueryExecutionId=query_response['QueryExecutionId'])
        query_status = queryExecution["QueryExecution"]["Status"]["State"]
        counter+=1
    return True if counter < 9 else False


import boto3
import time

# A helper function to create external table in athena if it doesnot exists
def create_delta_table_athena(curr_df,prev_df,catalog_database_name,catalog_table_name,external_table_ddl,results_loc):
    glue_client = boto3.client("glue")
    athena_client = boto3.client("athena")
    load_partition_query = '''MSCK REPAIR TABLE `{}`.`{}`'''.format(catalog_database_name,catalog_table_name)
    drop_table_query = '''DROP TABLE `{}`.`{}`'''.format(catalog_database_name,catalog_table_name)

    glue_response =glue_client.get_tables(DatabaseName = catalog_database_name)
    catalog_tables = [table_meta["Name"] for table_meta in glue_response["TableList"]]
    schema_match = len(curr_df.columns) == len(prev_df.columns) and all(col in prev_df.columns for col in curr_df.columns)
    
    if catalog_table_name not in catalog_tables:
        print("Creating Table : {}.{}".format(catalog_database_name,catalog_table_name))
        athena_ddl_response = execute_athena_query(external_table_ddl,catalog_database_name,results_loc)
        wait_response_ddl  = wait_for_query_execution(athena_ddl_response)
        if wait_response_ddl:
            print("Loading Partitions for {}.{}".format(catalog_database_name,catalog_table_name))
            athena_partitions_response = execute_athena_query(load_partition_query,catalog_database_name,results_loc)
            wait_response_partitions  = wait_for_query_execution(athena_partitions_response)
            if not wait_response_partitions:
                print("Loading Partitions failed for {}.{}".format(catalog_database_name,catalog_table_name))
        else:
            print("DDL failed for {}.{}".format(catalog_database_name,catalog_table_name))
                    
    elif not schema_match: 
        print("Dropping Table : {}.{}".format(catalog_database_name,catalog_table_name))
        athena_drop_response = execute_athena_query(drop_table_query,catalog_database_name,results_loc)
        wait_response_drop  = wait_for_query_execution(athena_drop_response)
        if wait_response_drop:
            print("Creating Table : {}.{}".format(catalog_database_name,catalog_table_name))
            athena_ddl_response = execute_athena_query(external_table_ddl,catalog_database_name,results_loc)
            wait_response_ddl  = wait_for_query_execution(athena_ddl_response)
            if wait_response_ddl:
                print("Loading Partitions for {}.{}".format(catalog_database_name,catalog_table_name))
                athena_partitions_response = execute_athena_query(load_partition_query,catalog_database_name,results_loc)
                wait_response_partitions  = wait_for_query_execution(athena_partitions_response)
                if not wait_response_partitions:
                    print("Loading Partitions failed for {}.{}".format(catalog_database_name,catalog_table_name))
            else:
                    print("DDL failed for {}.{}".format(catalog_database_name,catalog_table_name))          
        else:
             print("Drop failed for {}.{}".format(catalog_database_name,catalog_table_name))
    else:
        print("Loading Partitions for {}.{}".format(catalog_database_name,catalog_table_name))
        athena_partitions_response = execute_athena_query(load_partition_query,catalog_database_name,results_loc)
        wait_response_partitions  = wait_for_query_execution(athena_partitions_response)
        if not wait_response_partitions:
            print("Loading Partitions failed for {}.{}".format(catalog_database_name,catalog_table_name))
        

# A helper function to get the schema of the data present in the s3 target folder for cataloging in lake formation
def get_external_delta_ddl(df,partition_cols,database_name,table_name,
                                            serde_lib,input_format,output_format,manifest_loc):
    schema_ddl = ""
    partitions_ddl = ""
    delimiter = "\n"
    df = rearrange_schema(df,partition_cols)
    schema_swap = {"LongType" : "BIGINT", "IntegerType" : "INT"}
    for field in df.schema.fields:
        field_datatype = schema_swap[str(field.dataType)] if str(field.dataType) in list(schema_swap.keys()) else str(field.dataType).replace("Type","").upper()
        if field.name in partition_cols:
             partitions_ddl = partitions_ddl + str(field.name).lower() + " " + field_datatype + ","
        else:
            schema_ddl = schema_ddl + str(field.name).lower() + " " + field_datatype + "," + delimiter
    schema_ddl =  schema_ddl[0:-2]
    external_table_ddl = '''CREATE EXTERNAL TABLE IF NOT EXISTS `{}`.`{}` ('''.format(database_name,table_name) + delimiter \
                                     + schema_ddl + delimiter + ") PARTITIONED BY ({})".format(partitions_ddl[0:-1]) \
                                     + delimiter + "ROW FORMAT SERDE '{}'".format(serde_lib)  \
                                     + delimiter + "STORED AS INPUTFORMAT '{}'".format(input_format) \
                                     + delimiter + "OUTPUTFORMAT '{}'".format(output_format)\
                                     + delimiter + "LOCATION '{}'".format(manifest_loc)
    return external_table_ddl
    

#-----------------------------------------ETL Driver Code--------------------------------------------------------#


from delta import *
from pyspark.sql import SparkSession

# Creating the spark session for using spark transformations and actions
spark = SparkSession \
    .builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.delta.logStore.class","org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


# Reading the input configurations from s3 config file for the ETL process
input_config_path="s3://datalake-platform/dependencies/configurations/config_delta.json"

configs= spark.read\
                        .option("multiline",True)\
                        .json(input_config_path)\
                        .collect()[0]
SOURCE_S3_PATH = configs["source_s3_path"]
SINK_S3_PATH = configs["sink_s3_path"]
CATALOG_DB_NAME = configs["catalog_db_name"]
CATALOG_TABLE_NAME = configs["catalog_table_name"]
SERDE_LIBRARY = configs["serde_lib"]
INPUT_FORMAT = configs["input_format"]
OUTPUT_FORMAT = configs["output_format"]
S3_MANIFEST_PATH = configs["s3_manifest_path"]
ATHENA_RES_PATH = configs["s3_athena_query_result_path"]


# Reading file name from the s3 event based lambda function
import sys
from awsglue.utils import getResolvedOptions

job_arguments = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'source_file_path'])
SOURCE_S3_FILE_PATH = job_arguments["source_file_path"]
     

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Reading the input json files from the source s3 location
df = spark.read\
                 .option("inferSchema",True)\
                 .json(SOURCE_S3_FILE_PATH)\
                 .withColumn("input_file", input_file_name())


# Reading the previous version of data
prev_df = spark.read\
                          .format("delta")\
                          .load(SINK_S3_PATH)


# Flattening the input json df and creating partition columns from the input data
df_denormalized = flatten_json(df)\
                               .withColumn("row_insert_tsp",from_unixtime("tts"))\
                               .withColumn("row_insert_date",to_date("row_insert_tsp"))\
                               .withColumn("year",year(col("row_insert_date")))\
                               .withColumn("month",month(col("row_insert_date")))\
                               .withColumn("day",date_format(col("row_insert_date"),"d"))\
                               .withColumn("category",lower(col("category")))



# Defining the partition columns in order
partition_cols = ["customerid","vehicle","category","year","month","day"]


# Writing the flattened json input messages according to the defined partitions in delta format
df_denormalized.write\
                           .format("delta")\
                           .option("mergeSchema", True) \
                           .partitionBy(*partition_cols)\
                           .mode("append")\
                           .save(SINK_S3_PATH)


# Generate manifest file for making AdHoc queries in Athena/Catalog
deltaTable = DeltaTable.forPath(spark,SINK_S3_PATH)
deltaTable.generate("symlink_format_manifest")


import boto3

# Writing the latest changes if any to tables in Athena
curr_df = spark.read\
                          .format("delta")\
                          .load(SINK_S3_PATH)
external_table_ddl = get_external_delta_ddl(df_denormalized,partition_cols,CATALOG_DB_NAME,CATALOG_TABLE_NAME,
                                  SERDE_LIBRARY,INPUT_FORMAT,OUTPUT_FORMAT,S3_MANIFEST_PATH)
create_delta_table_athena(curr_df,prev_df,CATALOG_DB_NAME,CATALOG_TABLE_NAME,external_table_ddl,ATHENA_RES_PATH)



