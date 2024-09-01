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

# Script generated for node customer trusted
customertrusted_node1725232810112 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="customers_curated_san", transformation_ctx="customertrusted_node1725232810112")

# Script generated for node step training landing
steptraininglanding_node1725233344108 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="step_trainer_landing", transformation_ctx="steptraininglanding_node1725233344108")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT
    s.serialNumber,
    s.sensorReadingTime,
    s.distanceFromObject
FROM
    step_trainer_landing s
JOIN
    customers_curated c
ON
    s.serialNumber = c.serialnumber
GROUP BY
    s.serialNumber,
    s.sensorReadingTime,
    s.distanceFromObject;
'''
SQLQuery_node1725232884959 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customers_curated":customertrusted_node1725232810112, "step_trainer_landing":steptraininglanding_node1725233344108}, transformation_ctx = "SQLQuery_node1725232884959")

# Script generated for node out step_trainer_trusted
outstep_trainer_trusted_node1725232917362 = glueContext.getSink(path="s3://step-trainer-trusted-san/data/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="outstep_trainer_trusted_node1725232917362")
outstep_trainer_trusted_node1725232917362.setCatalogInfo(catalogDatabase="stedi_database",catalogTableName="step_trainer_trusted")
outstep_trainer_trusted_node1725232917362.setFormat("json")
outstep_trainer_trusted_node1725232917362.writeFrame(SQLQuery_node1725232884959)
job.commit()
