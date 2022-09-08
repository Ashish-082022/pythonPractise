from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="D:\\Study\\AVD class\\spark\\datasets\\asl.csv"
aslrdd=sc.textFile(data)
#aslrdd=spark.sparkContext.textFile()

#res=aslrdd.filter(lambda x:"age" not in x).map(lambda x:x.split(",")).filter(lambda x:"hyd" in x)
#select * from tab where city='hyd'
#res=aslrdd.map(lambda x:x.split(",")).filter(lambda x:"hyd" in x)

res=aslrdd.filter(lambda x:"age" not in x).map(lambda x:x.split(",")).map(lambda x:(x[2],1)).reduceByKey(lambda x,y:x+y)
#group by ur using ...cat based and something aggregation mandatory
#if u want to group the value first u must use reduceByKey ...its used to group the values.
#reduceByKey (any function/method ends with Key,means data must be(key,value )format
#reduceByKey ...based on same key,data process the values ..
#reduceByKey (x,y:x+y)

#in sql ... olap ...sel 8 from tab where ...
#group by col
#join tab
for i in res.collect():
    print(i)
    '''Map:apply a logic on top of each & every element ...how many input elements u have same number of output elements u ll get.
    filter:based on bool value filter the results.'''
