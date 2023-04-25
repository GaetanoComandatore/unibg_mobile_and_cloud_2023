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

tags_linkedin = readMongo("tags_linkedin","_id")
tags_xRef =readMongo("tags_xRef","_id")
tags_tedx = readMongo("tags_dataset","_id")
full_data = readMongo("full_data","_id")

##### linkedin.tag --> TEDx.tag (con la tabella di xReference)
tags_list = tags_xRef.join(tags_linkedin, tags_xRef.Linkedin == tags_linkedin.tag, "inner")\
    .select(col("TEDx"))\
    .distinct()
    
##### TEDx.tag --> TEDx.id (con la tabella tag_dataset)
eligible_list = tags_tedx.join(tags_list, tags_tedx.tag == tags_list.TEDx, "inner")\
    .select(col("idx").alias("_id"))\
    .distinct()

##### TEDx.id --> full data
eligible = eligible_list.join(full_data, full_data._id == eligible_list._id, "inner")

writeMongo(eligible,"eligible")