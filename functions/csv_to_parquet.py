import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1740936698947 = glueContext.create_dynamic_frame.from_catalog(database="aryannn15-cleaned-database", table_name="final_raw_statistics", transformation_ctx="AWSGlueDataCatalog_node1740936698947")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1740936716811 = glueContext.create_dynamic_frame.from_catalog(database="aryannn15-de-database", table_name="cleaned_statistics_reference_data_table", transformation_ctx="AWSGlueDataCatalog_node1740936716811")

# Script generated for node Join
Join_node1740936824010 = Join.apply(frame1=AWSGlueDataCatalog_node1740936698947, frame2=AWSGlueDataCatalog_node1740936716811, keys1=["category_id"], keys2=["id"], transformation_ctx="Join_node1740936824010")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1740936824010, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1740936691487", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1740937318077 = glueContext.getSink(path="s3://aryannn15-analytical-on-cleaned-data", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["region", "category_id"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1740937318077")
AmazonS3_node1740937318077.setCatalogInfo(catalogDatabase="aryannn15_analytical_db",catalogTableName="final_analytical")
AmazonS3_node1740937318077.setFormat("glueparquet", compression="snappy")
AmazonS3_node1740937318077.writeFrame(Join_node1740936824010)
job.commit()