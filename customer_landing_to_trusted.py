import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1702932464392 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://udacity-glue-nikhil/customer/landing/"]},
    transformation_ctx="CustomerLanding_node1702932464392",
)

# Script generated for node PrivacyFIlter
PrivacyFIlter_node1702932472114 = Filter.apply(
    frame=CustomerLanding_node1702932464392,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFIlter_node1702932472114",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1702932474948 = glueContext.write_dynamic_frame.from_options(
    frame=PrivacyFIlter_node1702932472114,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-glue-nikhil/customer/trusted/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node1702932474948",
)

job.commit()
