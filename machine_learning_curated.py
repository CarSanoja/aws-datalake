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

# Script generated for node step trainer trusted
steptrainertrusted_node1725233075484 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="step_trainer_trusted", transformation_ctx="steptrainertrusted_node1725233075484")

# Script generated for node acce trusted
accetrusted_node1725233076077 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="accelerometer_trusted_san", transformation_ctx="accetrusted_node1725233076077")

# Script generated for node customer curated
customercurated_node1725235296798 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="customer_trusted_san", transformation_ctx="customercurated_node1725235296798")

# Script generated for node SQL Query
SqlQuery1 = '''
SELECT
    ac.*,
    cc.*
FROM
    accelerometer_trusted ac
JOIN
    customers_curated cc
ON
    ac.user = cc.email
'''
SQLQuery_node1725235600865 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"accelerometer_trusted":accetrusted_node1725233076077, "customers_curated":customercurated_node1725235296798}, transformation_ctx = "SQLQuery_node1725235600865")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT
    st.serialNumber AS step_trainer_serialNumber,
    st.sensorReadingTime AS step_trainer_time,
    st.distanceFromObject,
    ac_curated.timeStamp AS accelerometer_time,
    ac_curated.x AS accelerometer_x,
    ac_curated.y AS accelerometer_y,
    ac_curated.z AS accelerometer_z
FROM
    step_trainer_trusted st
JOIN ac_curated
ON
    st.serialNumber = ac_curated.serialNumber
AND
    st.sensorReadingTime = ac_curated.timeStamp
GROUP BY
    st.serialNumber,
    st.sensorReadingTime,
    st.distanceFromObject,
    ac_curated.timeStamp,
    ac_curated.x,
    ac_curated.y,
    ac_curated.z;
'''
SQLQuery_node1725233120115 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":steptrainertrusted_node1725233075484, "ac_curated":SQLQuery_node1725235600865}, transformation_ctx = "SQLQuery_node1725233120115")

# Script generated for node Amazon S3
AmazonS3_node1725234272532 = glueContext.getSink(path="s3://machine-learning-curated-san/data/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1725234272532")
AmazonS3_node1725234272532.setCatalogInfo(catalogDatabase="stedi_database",catalogTableName="machine_learning_curated_san")
AmazonS3_node1725234272532.setFormat("json")
AmazonS3_node1725234272532.writeFrame(SQLQuery_node1725233120115)
job.commit()
