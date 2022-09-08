from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext
#data=[12,32,34,4,54,26]
#drdd=spark.sparkContext.parallelize(data)
data="D:\\Study\\AVD class\\spark\\datasets\\asl.csv"
aslrdd=sc.textFile(data)

#res=aslrdd.map(lambda x:x.split(",")).filter(lambda x:"blr" in x[2])
res=aslrdd.filter(lambda x:"age" not in x).map(lambda x:x.split(",")).toDF(["name","age","city"])
res.createOrReplaceTempView("tab") # dataframe ur giving one name ..its used to run sql queries on the top of dataframes.
#result=spark.sql("select * from tab where city='hyd' and age>30")
#result=spark.sql("select * from tab where city='blr' and age<30")
#result=res.where(col("age")>=30)
result=res.where((col("age")>=30) & (col("city")=="mas"))
result.show()
#above steps mostly use in 2015 after march.. above approch .. rdd ur convert to dataframe.
#but after 2016 jan mostly use dataframe api
