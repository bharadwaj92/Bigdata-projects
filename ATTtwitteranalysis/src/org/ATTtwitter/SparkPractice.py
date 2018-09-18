'''
Created by bharadwaj on 9/17/2018

Enter Description :

'''
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("MyFirstApp").setMaster("local")
sc = SparkContext(conf= conf)