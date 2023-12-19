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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1703010659440 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-glue-nikhil/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1703010659440",
)

# Script generated for node Customer Landing
CustomerLanding_node1703010658807 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://udacity-glue-nikhil/customer/landing/customer-1691348231425.json"
        ],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1703010658807",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1703010658393 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-glue-nikhil/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1703010658393",
)

# Script generated for node Renamed keys for Customer Step Trainer Join
RenamedkeysforCustomerStepTrainerJoin_node1703010928298 = ApplyMapping.apply(
    frame=StepTrainerLanding_node1703010659440,
    mappings=[
        ("sensorreadingtime", "long", "sensorreadingtime", "long"),
        ("serialnumber", "string", "serialnumber", "string"),
        ("distancefromobject", "int", "distancefromobject", "int"),
    ],
    transformation_ctx="RenamedkeysforCustomerStepTrainerJoin_node1703010928298",
)

# Script generated for node SQL Query
SqlQuery1528 = """
select * from customer
where customer.shareWithResearchAsOfDate != 0;
"""
SQLQuery_node1703010671242 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1528,
    mapping={"customer": CustomerLanding_node1703010658807},
    transformation_ctx="SQLQuery_node1703010671242",
)

# Script generated for node Accelerometer Customer Join
AccelerometerCustomerJoin_node1703010677327 = Join.apply(
    frame1=SQLQuery_node1703010671242,
    frame2=AccelerometerLanding_node1703010658393,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="AccelerometerCustomerJoin_node1703010677327",
)

# Script generated for node Drop Fields For Customer Curated
DropFieldsForCustomerCurated_node1703010680892 = DropFields.apply(
    frame=AccelerometerCustomerJoin_node1703010677327,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFieldsForCustomerCurated_node1703010680892",
)

# Script generated for node Customer Step Trainer Join
CustomerStepTrainerJoin_node1703010684441 = Join.apply(
    frame1=DropFieldsForCustomerCurated_node1703010680892,
    frame2=RenamedkeysforCustomerStepTrainerJoin_node1703010928298,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="CustomerStepTrainerJoin_node1703010684441",
)

# Script generated for node Drop Fields For Step Trainer Trusted
DropFieldsForStepTrainerTrusted_node1703010689840 = DropFields.apply(
    frame=CustomerStepTrainerJoin_node1703010684441,
    paths=[
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
        "birthDay",
        "shareWithPublicAsOfDate",
        "serialNumber",
    ],
    transformation_ctx="DropFieldsForStepTrainerTrusted_node1703010689840",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1703010692424 = glueContext.write_dynamic_frame.from_options(
    frame=DropFieldsForStepTrainerTrusted_node1703010689840,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-glue-nikhil/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1703010692424",
)

job.commit()
