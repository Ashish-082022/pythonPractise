from pyspark.sql import *
from pyspark.sql.functions import *

spark=SparkSession.builder.master("local[*]").appName("test").getOrCreate()
rdd=spark.sparkContext.parallelize(array(1,2,3,4,5,6,7,8,9,10))
rdd.collect()
for x in rdd:print(x)
