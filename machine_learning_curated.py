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
AccelerometerLanding_node1703027301491 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-glue-nikhil/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1703027301491",
)

# Script generated for node Customer Landing
CustomerLanding_node1703027302264 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://udacity-glue-nikhil/customer/landing/customer-1691348231425.json"
        ],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1703027302264",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1703027635771 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1703027635771",
)

# Script generated for node SQL Query
SqlQuery1366 = """
select * from customer
where customer.shareWithResearchAsOfDate != 0;

"""
SQLQuery_node1703027309899 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1366,
    mapping={"customer": CustomerLanding_node1703027302264},
    transformation_ctx="SQLQuery_node1703027309899",
)

# Script generated for node Accelerometer Customer Join
AccelerometerCustomerJoin_node1703027540269 = Join.apply(
    frame1=AccelerometerLanding_node1703027301491,
    frame2=SQLQuery_node1703027309899,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="AccelerometerCustomerJoin_node1703027540269",
)

# Script generated for node Drop Fields
DropFields_node1703027314164 = DropFields.apply(
    frame=AccelerometerCustomerJoin_node1703027540269,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "serialNumber",
        "registrationDate",
        "lastUpdateDate",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1703027314164",
)

# Script generated for node Accelerometer Trainer Join
SqlQuery1367 = """
select * from trainer
join accelerometer on 
trainer.sensorreadingtime = accelerometer.timestamp;

"""
AccelerometerTrainerJoin_node1703027672107 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1367,
    mapping={
        "trainer": StepTrainerTrusted_node1703027635771,
        "accelerometer": DropFields_node1703027314164,
    },
    transformation_ctx="AccelerometerTrainerJoin_node1703027672107",
)

# Script generated for node Drop Fields
DropFields_node1703027789600 = DropFields.apply(
    frame=AccelerometerTrainerJoin_node1703027672107,
    paths=["user", "timestamp"],
    transformation_ctx="DropFields_node1703027789600",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1703027811096 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1703027789600,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-glue-nikhil/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node1703027811096",
)

job.commit()
