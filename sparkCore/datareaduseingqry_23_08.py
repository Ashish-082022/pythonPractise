from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
sc = spark.sparkContext
host="jdbc:mysql://sparkpoc.c8v9olldtsvz.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
qry="(select * from asl where age>50) t"
df=spark.read.format("jdbc").option("url",host).option("user","myuser").option("password","mypassword")\
    .option("dbtable",qry).option("driver","com.mysql.jdbc.Driver").load()
df.show()