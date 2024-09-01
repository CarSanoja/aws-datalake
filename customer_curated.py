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
accetrusted_node1725231051151 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="accelerometer_trusted_san", transformation_ctx="accetrusted_node1725231051151")

# Script generated for node customer trusted
customertrusted_node1725231129721 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="customer_trusted_san", transformation_ctx="customertrusted_node1725231129721")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT
        c.serialnumber,
        c.sharewithpublicasofdate,
        c.birthday,
        c.registrationdate,
        c.sharewithresearchasofdate,
        c.customername,
        c.email,
        c.lastupdatedate,
        c.phone,
        c.sharewithfriendsasofdate
    FROM
        customer_trusted c
    JOIN
        accelerometer_trusted a
    ON
        c.email = a.user
    WHERE
        c.sharewithresearchasofdate IS NOT NULL
    GROUP BY
        c.serialnumber,
        c.sharewithpublicasofdate,
        c.birthday,
        c.registrationdate,
        c.sharewithresearchasofdate,
        c.customername,
        c.email,
        c.lastupdatedate,
        c.phone,
        c.sharewithfriendsasofdate
'''
SQLQuery_node1725231145413 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_trusted":customertrusted_node1725231129721, "accelerometer_trusted":accetrusted_node1725231051151}, transformation_ctx = "SQLQuery_node1725231145413")

# Script generated for node Amazon S3
AmazonS3_node1725231256271 = glueContext.getSink(path="s3://customers-curated-san/data/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1725231256271")
AmazonS3_node1725231256271.setCatalogInfo(catalogDatabase="stedi_database",catalogTableName="customers_curated_san")
AmazonS3_node1725231256271.setFormat("json")
AmazonS3_node1725231256271.writeFrame(SQLQuery_node1725231145413)
job.commit()
