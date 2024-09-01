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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1725225470727 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="customer_landing_san", transformation_ctx="AWSGlueDataCatalog_node1725225470727")

# Script generated for node SQL Query
SqlQuery0 = '''
select * FROM myDataSource
WHERE sharewithresearchasofdate IS NOT NULL
'''
SQLQuery_node1725225495619 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AWSGlueDataCatalog_node1725225470727}, transformation_ctx = "SQLQuery_node1725225495619")

# Script generated for node Amazon S3
AmazonS3_node1725225783294 = glueContext.getSink(path="s3://customer-trusted-san/data/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1725225783294")
AmazonS3_node1725225783294.setCatalogInfo(catalogDatabase="stedi_database",catalogTableName="customer_trusted_san")
AmazonS3_node1725225783294.setFormat("json")
AmazonS3_node1725225783294.writeFrame(SQLQuery_node1725225495619)
job.commit()
