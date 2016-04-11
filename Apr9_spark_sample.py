%pyspark

from pyspark import SparkContext
from pyspark.sql import SQLContext

import json

AWS_KEY, AWS_SECRET = "******************"
sqlContext.clearCache()
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", AWS_KEY)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", AWS_SECRET)

sqlContext = SQLContext(sc)

twitter_raw =  "s3n://datalaus/twitter-data/tweets.txt"

tw_rdd = sc.textFile(twitter_raw)
tw_json = tw_rdd.map(lambda x: json.loads(x))

df_tw = sqlContext.read.option("samplingRatio", "0.5").json(twitter_raw)
df_tw.show()