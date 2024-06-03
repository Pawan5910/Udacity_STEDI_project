import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1717362851198 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1717362851198")

# Script generated for node Customer Curated
CustomerCurated_node1717362853067 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1717362853067")

# Script generated for node SQL Query
SqlQuery326 = '''
SELECT 
    stl.serialnumber, 
    stl.sensorreadingtime, 
    stl.distancefromobject 
FROM 
    cc
JOIN 
    stl 
ON 
    cc.serialnumber = stl.serialnumber;

'''
SQLQuery_node1717363001826 = sparkSqlQuery(glueContext, query = SqlQuery326, mapping = {"stl":StepTrainerLanding_node1717362851198, "cc":CustomerCurated_node1717362853067}, transformation_ctx = "SQLQuery_node1717363001826")

# Script generated for node Amazon S3
AmazonS3_node1717363146292 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1717363001826, connection_type="s3", format="json", connection_options={"path": "s3://paw-lake-house/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1717363146292")

job.commit()