from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
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
#tabs=['dept','donations','emp','territories','customers','us_500_csv']
qry="(select table_name from information_schema.TABLEs where TABLE_SCHEMA='mysqldb') t"
#host="jdbc:mysql://sparkpoc.c8v9olldtsvz.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
df1=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd)\
    .option("dbtable",qry).option("driver","com.mysql.jdbc.Driver").load()
tabs=[x[0] for x in df1.collect() if df1.count()>0]

for i in tabs:
    df=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd)\
    .option("dbtable",i).option("driver","com.mysql.jdbc.Driver").load()
    df.show(5)