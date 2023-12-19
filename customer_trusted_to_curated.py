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


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1702933149430 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1702933149430",
)

# Script generated for node Customer Landing
CustomerLanding_node1702933150462 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://udacity-glue-nikhil/customer/landing/"]},
    transformation_ctx="CustomerLanding_node1702933150462",
)

# Script generated for node SQL Query
SqlQuery1105 = """
select * from customer_landing 
where customer_landing.shareWithResearchAsOfDate != 0;
"""
SQLQuery_node1702933641170 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1105,
    mapping={"customer_landing": CustomerLanding_node1702933150462},
    transformation_ctx="SQLQuery_node1702933641170",
)

# Script generated for node Join
Join_node1702933167126 = Join.apply(
    frame1=AccelerometerLanding_node1702933149430,
    frame2=SQLQuery_node1702933641170,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1702933167126",
)

# Script generated for node Drop Fields
DropFields_node1702933169560 = DropFields.apply(
    frame=Join_node1702933167126,
    paths=["timestamp", "x", "user", "y", "z"],
    transformation_ctx="DropFields_node1702933169560",
)

# Script generated for node Customer Curated
CustomerCurated_node1702933173227 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1702933169560,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-glue-nikhil/customer/curated/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node1702933173227",
)

job.commit()
