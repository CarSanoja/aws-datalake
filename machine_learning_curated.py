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

# Script generated for node acce trusted
accetrusted_node1725233076077 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="accelerometer_trusted_san", transformation_ctx="accetrusted_node1725233076077")

# Script generated for node step trainer trusted
steptrainertrusted_node1725233075484 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="step_trainer_trusted", transformation_ctx="steptrainertrusted_node1725233075484")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT
    st.serialNumber AS step_trainer_serialNumber,
    st.sensorReadingTime AS step_trainer_time,
    st.distanceFromObject,
    ac.timeStamp AS accelerometer_time,
    ac.x AS accelerometer_x,
    ac.y AS accelerometer_y,
    ac.z AS accelerometer_z
FROM
    (SELECT 
        sensorReadingTime, 
        serialNumber, 
        distanceFromObject 
     FROM 
        step_trainer_trusted 
     WHERE 
        sensorReadingTime IS NOT NULL) st
JOIN
    (SELECT 
        timeStamp, 
        user, 
        x, 
        y, 
        z 
     FROM 
        accelerometer_trusted 
     WHERE 
        timeStamp IS NOT NULL) ac
ON
    st.sensorReadingTime = ac.timeStamp
WHERE
    ac.user IN (SELECT email FROM customers_curated)
;
'''
SQLQuery_node1725233120115 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":steptrainertrusted_node1725233075484, "accelerometer_trusted":accetrusted_node1725233076077}, transformation_ctx = "SQLQuery_node1725233120115")

# Script generated for node SQL Query
SqlQuery1 = '''
SELECT 'Curated', 'Machine Learning', COUNT(*) FROM myDataSource;
'''
SQLQuery_node1725238257579 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"myDataSource":SQLQuery_node1725233120115}, transformation_ctx = "SQLQuery_node1725238257579")

# Script generated for node Amazon S3
AmazonS3_node1725234272532 = glueContext.getSink(path="s3://machine-learning-curated-san/data/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1725234272532")
AmazonS3_node1725234272532.setCatalogInfo(catalogDatabase="stedi_database",catalogTableName="machine_learning_curated_san")
AmazonS3_node1725234272532.setFormat("json")
AmazonS3_node1725234272532.writeFrame(SQLQuery_node1725233120115)
job.commit()
