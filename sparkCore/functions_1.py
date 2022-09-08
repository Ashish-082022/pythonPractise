from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="D:\\Study\\AVD class\\spark\\datasets\\us-500.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#ndf=df.groupBy(df.state).agg(count("*").alias("cnt"),collect_list(df.first_name).alias("names")).orderBy(col("cnt").desc())
# #Here collect_list shows first_name if duplicates ll show it
#ndf=df.groupBy(df.state).agg(count(col("city")).alias("cnt"),collect_set(df.city).alias("cities")).orderBy(col("cnt").desc())
# #Here collect_set ..removes duplicate cities in cities column
#ndf=df.withColumn("state",when(col("state")=="NY","NewYork").otherwise(col("state")))
#if found NY then replace with NewYork otherwise remainig col data
#ndf=df.withColumn("state",when(col("state")=="NY","NewYork").when(col("state")=="CA","California").otherwise(col("state")))
#ndf=df.withColumn("address1",when(col("address").contains("#"),"*****").otherwise("address")).withColumn("address2",regexp_replace(col("address"),"#","_"))
#ndf1=df.withColumn("substr",substring(col("email"),0,5)).withColumn("username",substring_index(col("email"),"@",1)).withColumn("emails",substring_index(col("email"),"@",-1))
#substring ..syn:first col name,start with,no of char  #substring_index...syn:col name,str,if we want left of str the write any positive no if right side data then give negative no
#ndf=ndf1.groupBy(col("emails")).count().orderBy(col("count").desc())
#Here grouping with emails count and count column showing by desc order
#ndf.show()
#ndf.printSchema()

#using sql queries
df.createOrReplaceTempView("tab")
#ndf=spark.sql("select *, concat_ws('_',first_name,last_name) fullname from tab")
#ndf=spark.sql("select *, concat_ws('_',first_name,last_name) fullname,substring_index(email,'@',-1) mail from tab")
qry='''with tmp as (select *, concat_ws('_',first_name,last_name) fullname,substring_index(email,'@',-1) mail from tab)
select mail, count(*) cnt from tmp group by mail order by cnt desc'''
#ndf=spark.sql(qry)
#ndf.show()
#ndf.printSchema()

#create ur own functions
def func(st):
    if(st=="NY"):return "30% off"
    elif(st=="CA"):return "40% off"
    elif(st=="OH"):return "50% off"
    else:return "500/- off"
#by default spark unable to understand python functions.so convert python/scala/java function to UDF(spark able to understant udfs)
'''uf=udf(func)
ndf=df.withColumn("offer",uf(col("state")))
ndf.printSchema()
ndf.show(truncate=False)
'''
uf=udf(func)
spark.udf.register("offer",uf) #user define fuctions convert to sql function
ndf=spark.sql("select *,offer(state) todayoffers from tab")
ndf.printSchema()
ndf.show(truncate=False)