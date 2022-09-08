from pyspark.sql import *
from pyspark.sql.functions import *
import configparser
from configparser import ConfigParser
conf=ConfigParser()
conf.read(r"D:\\bigdata\\config.txt")
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","pass")
data=conf.get("input","data")
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",",").load(data)
import re
cols =[re.sub('[^a-zA-Z0-9]',"",c.lower()) for c in df.columns]
ndf = df.toDF(*cols)

ndf.write.mode("overwrite").format("jdbc").option("url",host).option("user",user).option("password",pwd)\
    .option("dbtable","territories").option("driver","com.mysql.jdbc.Driver").save()
ndf.show(5)
ndf.printSchema()