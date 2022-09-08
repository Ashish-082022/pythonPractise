from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

sc.setLogLevel("ERROR")
host="jdbc:mysql://sparkpoc.c8v9olldtsvz.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
uname="myuser"
pwd="mypassword"
df=spark.read.format('jdbc').option('url',host).option('user',uname)\
    .option('password',pwd).option('driver','com.mysql.jdbc.Driver')\
    .option('dbtable','us_500_csv').load()
df.show(5)
df1=spark.read.format('jdbc').option('url',host).option('user',uname)\
    .option('password',pwd).option('driver','com.mysql.jdbc.Driver')\
    .option('dbtable','emp').load()
df1.show(7)
