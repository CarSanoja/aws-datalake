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

# Script generated for node Accelerometer
Accelerometer_node1725229111170 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="accelerometer_landing_san", transformation_ctx="Accelerometer_node1725229111170")

# Script generated for node Trusted
Trusted_node1725229111793 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="customer_trusted_san", transformation_ctx="Trusted_node1725229111793")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT a.*
FROM accelerometer_landing a
JOIN customer_trusted c ON a.user = c.email
WHERE c.sharewithresearchasofdate IS NOT NULL
'''
SQLQuery_node1725229754363 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_landing":Accelerometer_node1725229111170, "customer_trusted":Trusted_node1725229111793}, transformation_ctx = "SQLQuery_node1725229754363")

# Script generated for node Amazon S3
AmazonS3_node1725229223161 = glueContext.getSink(path="s3://accelerometer-trusted-san/data/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1725229223161")
AmazonS3_node1725229223161.setCatalogInfo(catalogDatabase="stedi_database",catalogTableName="accelerometer-trusted-san")
AmazonS3_node1725229223161.setFormat("json")
AmazonS3_node1725229223161.writeFrame(SQLQuery_node1725229754363)
job.commit()
