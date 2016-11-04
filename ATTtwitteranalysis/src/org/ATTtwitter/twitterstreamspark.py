from pyspark import SparkConf
from pyspark.sql import SparkSession
from dateutil import parser
import datetime
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StringType , TimestampType 
import numpy as np
import pandas as pd   
name = 'created_at'
td2sqld = UserDefinedFunction(lambda x: parser.parse(x).replace(tzinfo = None).isoformat() ,StringType())
strtodate = UserDefinedFunction(lambda x: datetime.datetime.strptime(x,'%Y-%m-%dT%H:%M:%S'),TimestampType())
conf = SparkConf().setAppName("learning").setMaster("local[4]")
spark = SparkSession.builder.getOrCreate()
twitterdata = spark.read.json('ATT.json')
td_new = twitterdata.select(*[strtodate(td2sqld(column)).alias(name) if column == name else column for column in twitterdata.columns])
td_new.createOrReplaceTempView("table1")
tempdata = spark.sql("select created_at,id, text,place.country,place.name  from table1 where quoted_status IS NOT NULL and lang = 'en' ")
tempdata.persist()
finaldf = tempdata.toPandas()
print(type(finaldf))
print(finaldf[:5])
