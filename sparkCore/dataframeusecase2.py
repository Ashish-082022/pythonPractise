from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="D:\\Study\\AVD class\\spark\\datasets\\bank-full.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",";").load(data)
#sep option used to specify delimiter
#By default spark every field consider as string, but i want to change columns appropriate datatype like int, double , string use inferschema ,true option.
#if u not mention like this 1000+1000...if int ...2000 if sting, u will get 10001000

#data processing programming friendly
#res=df.where(col("age")>90)
#res=df.where((col("age")>60) & (col("marital")!="married"))
#res=df.select(col("age"),col("marital"),col("balance")).where((col("age")>60) & (col("marital")!="married"))
#res=df.where((col("age")>60) & (col("marital")!="married") & (col("balance")>=40000))
#res=df.where((col("age")>60) & (col("marital")=="married") & (col("balance")>=40000))
#res=df.where((col("age")>60) | (col("marital")=="married") & (col("balance")>=40000))
#res=df.where(((col("age")>60) | (col("marital")=="married")) & (col("balance")>=40000))
#res.show()
#res.printSchema()
#res = df.groupBy(col("marital")).agg(sum(col("balance")).alias("smb")).orderBy(col("smb").desc())
#res = df.groupBy(col("marital")).count()
#res = df.groupBy(col("marital")).agg(count("*")).alias("cnt"),sum(col("balance")).alias("smb").orderBy(col("smb").desc())#Here got Error
res=df.groupBy(col("marital")).agg(count("*").alias("cnt"),sum(col("balance")).alias("smb"))

#process sql friendly
df.createOrReplaceTempView("tab")
#createOrReplaceTempView ...register this dataframe as a table.its very useful to run sql queries.
#res=spark.sql("select * from tab where age>60 and balance >50000")
#married, how much balance they have
#divorced, how much balance they have
#res=spark.sql("select marital,sum(balance) sumbal from tab group by(marital)")
res.show()