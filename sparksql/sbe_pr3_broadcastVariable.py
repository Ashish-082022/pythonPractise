from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
states = {"NY":"New York","CA":"California","FL":"Florida"}
broadcastStates = spark.sparkContext.broadcast(states)
data = [("James","Smith","USA","CA"),
        ("Michael","Rose","USA","NY"),
        ("Robert","Williams","USA","CA"),
        ("Maria","Jones","USA","FL")
        ]
columns=["firstname","lastname","country","state"]
df = spark.createDataFrame(data=data,schema=columns)

df.printSchema()
df.show(truncate=False)
def state_convert(code):
    return broadcastStates.value[code]
result = df.rdd.map(lambda x : (x[0],x[1],x[2],state_convert(x[3]))).toDF(columns)
result.show(truncate=False)

#filterDf = df.where((df['state'].isin(broadcastStates.value)))
#filterDf.show()
