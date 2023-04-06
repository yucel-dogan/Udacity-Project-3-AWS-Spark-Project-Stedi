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

# Script generated for node customers
customers_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-spark-project-yuceld/customers/customers-landing/"],
        "recurse": True,
    },
    transformation_ctx="customers_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Filter.apply(
    frame=customers_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node customers-trusted
customerstrusted_node3 = glueContext.getSink(
    path="s3://udacity-spark-project-yuceld/customers/customers-trusted-zone/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customerstrusted_node3",
)
customerstrusted_node3.setCatalogInfo(
    catalogDatabase="udacity-test-database", catalogTableName="customers-trusted-zone"
)
customerstrusted_node3.setFormat("json")
customerstrusted_node3.writeFrame(ApplyMapping_node2)
job.commit()
