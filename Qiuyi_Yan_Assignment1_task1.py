from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles, 
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

sc = SparkContext(appName="Assignment-1")
lines = sc.textFile(sys.argv[1])
linesHeader = lines.first()
header = sc.parallelize([linesHeader])
linesWithOutHeader = lines.subtract(header)
taxis = linesWithOutHeader.map(lambda x: x.split(','))
mainTaxisData = taxis.map(lambda p: (p[0], p[1] , p[2] , p[3], p[4] , p[5] , p[6], p[7] , p[8] , p[9], p[10], p[11], p[12] , p[13], p[14], p[15], p[16]))
#Task 1
#Your code goes here
rdd1=mainTaxisData.map(lambda p: (p[0],p[1]) )
#print(rdd1.collect())
rdd2 = rdd1.map(lambda x: (x,1))
#print(rdd2.collect())

rdd3 = rdd2.reduceByKey(lambda x, y : x+y)
#print(rdd2.collect())

rdd4 = rdd3.map(lambda p: (p[0][0],p[1]))
#print(rdd4.collect())

top = rdd4.top(10, lambda x:x[1])
#print(top)
results_1 = top
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

# 假设你的数据是这样的
data = results_1

# 将 list 转换为 RDD
rdd00 = sc.parallelize(data)
results_1 = rdd00
print(type(results_1))
results_1.coalesce(1).saveAsTextFile(sys.argv[2])





sc.stop()