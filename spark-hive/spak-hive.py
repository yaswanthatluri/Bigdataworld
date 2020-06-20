from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession \
    .builder \
    .appName("creating hive orc table from sopark") \
    .enableHiveSupport() \
    .getOrCreate()

rawdf= spark.read.format("csv").option("inferSchema","true").option("header","true").load("s3://bucket/text.csv")

rawdf.registerTempTable("orc_table")

#any sql commands
spark.sql("CREATE TABLE createdfromspark_orc STORED AS ORC AS SELECT * from orc_table")

tabledf = sqlContext.sql("show tables").collect()

print(tabledf)


#External table in hive
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession \
    .builder \
    .appName("creating hive orc table from sopark") \
    .enableHiveSupport() \
    .getOrCreate()

rawdf= spark.read.format("csv").option("inferSchema","true").option("header","true").load("s3://yash-east/casedata/bostoncrime/crime.csv")

rawdf.write.format("orc").save("s3://yash-east/casedata/bostoncrime/output1")

orcdf = spark.read.format("orc").load("s3://yash-east/casedata/bostoncrime/output1")

orcdf.printSchema()

orcdf.show(5)

spark.sql("""create external table default.sachinorc(
INCIDENT_NUMBER string, 
OFFENSE_CODE int, 
OFFENSE_CODE_GROUP string, 
OFFENSE_DESCRIPTION string,
DISTRICT string, 
REPORTING_AREA string, 
SHOOTING string, 
OCCURRED_ON_DATE string, 
YEAR int, 
MONTH int, 
DAY_OF_WEEK string, 
HOUR int,
UCR_PART string, 
STREET string, 
Lat double, 
Long double, 
Location string)
STORED AS ORC
location "s3://yash-east/casedata/bostoncrime/output1"
TBLPROPERTIES ("orc.compress"="SNAPPY")""")

sqlContext.sql("select * from extorc").show(5)



