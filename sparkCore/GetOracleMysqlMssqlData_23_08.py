from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
host="jdbc:mysql://sparkpoc.c8v9olldtsvz.ap-south-1.rds.amazonaws.com:3306/mysqldb"
uname="myuser"
pwd="mypassword"
df=spark.read.format("jdbc").option("url",host)\
    .option("dbtable","emp").option("user",uname).option("password",pwd)\
    .option("driver","com.mysql.jdbc.Driver").load()
df.show()