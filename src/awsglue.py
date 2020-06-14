import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from awsglue.dynamicframe import DynamicFrame, ResolveOption
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, IntegerType, LongType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "plex_data", table_name = "catalog_channels", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "plex_data", table_name = "catalogscatalogs", transformation_ctx = "datasource0")
## Custom ETL PySpark
df = datasource0.toDF()


catalog_mappings = df.select(explode(df.MediaContainer.Directory).alias("directories") )
catalog_mappings.printSchema()

get_paths = udf(lambda channel_loc: ','.join(list( map(lambda loc: loc.path, channel_loc) )), StringType())
catalog_mappings = catalog_mappings.withColumn("pathlist", get_paths(catalog_mappings.directory.Location))
                     
    


## @type: ApplyMapping
## @args: [mapping = [("channels.allowsync", "boolean", "channels.allowsync", "boolean"), ("channels.art", "string", "channels.art", "string"), ("channels.composite", "string", "channels.composite", "string"), ("channels.filters", "boolean", "channels.filters", "boolean"), ("channels.refreshing", "boolean", "channels.refreshing", "boolean"), ("channels.thumb", "string", "channels.thumb", "string"), ("channels.key", "string", "channels.key", "string"), ("channels.type", "string", "channels.type", "string"), ("channels.title", "string", "channels.title", "string"), ("channels.agent", "string", "channels.agent", "string"), ("channels.scanner", "string", "channels.scanner", "string"), ("channels.language", "string", "channels.language", "string"), ("channels.uuid", "string", "channels.uuid", "string"), ("channels.updatedat", "int", "channels.updatedat", "int"), ("channels.createdat", "int", "channels.createdat", "int"), ("channels.scannedat", "int", "channels.scannedat", "int"), ("channels.content", "boolean", "channels.content", "boolean"), ("channels.directory", "boolean", "channels.directory", "boolean"), ("channels.contentchangedat", "int", "channels.contentchangedat", "int"), ("channels.location", "array", "channels.location", "array")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
datasource0 = DynamicFrame.fromDF(catalog_mappings, glueContext, "nested")
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [
    ("directories.allowsync", "boolean", "allowsync", "boolean"), 
    ("directories.art", "string", "art", "string"), 
    ("directories.composite", "string", "composite", "string"), 
    ("directories.filters", "boolean", "filters", "boolean"), 
    ("directories.refreshing", "boolean", "refreshing", "boolean"), 
    ("directories.thumb", "string", "thumb", "string"), 
    ("directories.key", "string", "key", "string"), 
    ("directories.type", "string", "type", "string"), 
    ("directories.title", "string", "title", "string"), 
    ("directories.agent", "string", "agent", "string"), 
    ("directories.scanner", "string", "scanner", "string"), 
    ("directories.language", "string", "language", "string"), 
    ("directories.uuid", "string", "uuid", "string"), 
    ("directories.updatedat", "int", "updatedat", "int"), 
    ("directories.createdat", "int", "createdat", "int"), 
    ("directories.scannedat", "int", "scannedat", "int"), 
    ("directories.content", "boolean", "content", "boolean"), 
    ("directories.directory", "boolean", "directory", "boolean"), 
    ("directories.contentchangedat", "int", "contentchangedat", "int"), 
    ( "pathlist", "array", "location", "array")
], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://lsft-athena-investigations/output/catalog"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields3, connection_type = "s3", connection_options = {"path": "s3://lsft-athena-investigations/output/catalog"}, format = "parquet", transformation_ctx = "datasink4")
job.commit()