from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import getpass
username = getpass.getuser()

spark = SparkSession.builder.config('spark.ui.port', '0').master("yarn").appName("test_CarSales").getOrCreate()

df=spark.read.csv('/user/jpvir/data/data.csv',
                  schema='''
				            incident_id INT,
                            incident_type STRING,
                            vin_number STRING,
                            make STRING,
                            model STRING,
                            year STRING,
                            incident_date STRING,
                            description STRING
                         ''')

initDF = df.sort('vin_number').filter('incident_type == "I"').select('incident_type','vin_number','make','year')
acctDF = df.sort('vin_number').filter('incident_type == "A"').select('incident_type','vin_number','make','year')
joinDF=initDF.join(acctDF, 'vin_number').select(initDF.make, initDF.year)
rptDF=joinDF.sort('make','year').groupBy("make","year").count()
rptDF.write.format("com.databricks.spark.csv").save("/user/jpvir/data/output.csv")
