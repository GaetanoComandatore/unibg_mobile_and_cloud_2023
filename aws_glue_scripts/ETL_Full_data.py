# codice per funzione Glue scritto in python
# codice nel JOB ETL_Full_Data

###### TEDx-Load-Aggregate-Model ######

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, collect_set, array_join, struct

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

########### READ + WRITE on AWS S3 ###########
bucketName="s3://provadati/"
#------------------------------------------
def readS3(fileName,keyName):
    dataset= spark.read \
        .option("header","true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .csv(bucketName+fileName)
    return dataset.filter(keyName + " is not null")
#------------------------------------------
def writeS3(setName,fileName):
    setName.write \
        .option("header","true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .json(bucketName+fileName)
        #.mode(SaveMode.Overwrite) # oppure Append oppure Ignore oppure ErrorIfExists

######## READ + WRITE on MongoDB ATLAS ########
uriMongo = "mongodb+srv://gcomandatore:gigia@cluster0.tph4goj.mongodb.net"
DBName = "linktedin"
#------------------------------------------
def readMongo(collectionName,keyName):
    dataset = spark.read\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri", uriMongo)\
        .option("database", DBName)\
        .option("collection", collectionName)\
        .option("ssl", "true")\
        .option("ssl.domain_match", "false")\
        .option("header","true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .load()
    return dataset.filter(keyName + " is not null")
#------------------------------------------    
def writeMongo(setName,collectionName):
    write_mongo_options = {
        "uri": uriMongo,
        "database": DBName,
        "collection": collectionName,
        "ssl": "true",
        "ssl.domain_match": "false"}
    setName_frame = DynamicFrame.fromDF(setName, glueContext, "nested")
    glueContext.write_dynamic_frame \
        .from_options(setName_frame, connection_type="mongodb", connection_options=write_mongo_options)
##############################################

tedx = readMongo("tedx_dataset","idx").drop("_id") #_id non serve
tags = readMongo("tags_dataset","idx").drop("_id") #_id non serve
watchNext = readMongo("watch_next_dataset","idx").drop("_id") #_id non serve
#tedx = readS3("tedx_dataset.csv","idx") #_id non c'è
#tags = readS3("tags_dataset.csv","idx") #_id non c'è
#watchNext = readS3("watch_next_dataset.csv","idx")  #_id non c'è

### CREATE THE AGGREGATE MODEL tags_agg
tags_agg = tags.groupBy(col("idx").alias("idx_ref")).agg(collect_set("tag").alias("tags"))

### tedx_agg = tags_agg + tedx
tedx_agg = tedx.join(tags_agg, tedx.idx == tags_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \

### CREATE THE STRUCTURED MODEL watchNext_str
watchNext_str = watchNext.withColumn("next",struct(col("watch_next_idx").alias("id"),col("url").alias("url")))

### CREATE THE AGGREGATE MODEL watchNext_agg
watchNext_agg = watchNext_str.groupBy(col("idx").alias("idx_ref")).agg(collect_set("next").alias("next"))

### full_agg = tedx_agg + watchNext_agg
full_agg = tedx_agg.join(watchNext_agg, tedx_agg._id == watchNext_agg.idx_ref, "left") \
    .select(col("*")) \
    .drop("idx_ref")

writeMongo(full_agg,"full_data")
#writeS3(full_agg,"full_data.json")