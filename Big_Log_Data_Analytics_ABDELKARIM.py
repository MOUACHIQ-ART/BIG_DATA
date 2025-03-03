
# ABDELKARIM MOUACHIQ
# Big Log Data Analytics
"""

#install Java8
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
#download spark3.5.4
!wget -q https://archive.apache.org/dist/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz
#!wget -q https://archive.apache.org/dist/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
#unzip it
!tar xf spark-3.5.4-bin-hadoop3.tgz
#!tar xf spark-3.2.1-bin-hadoop3.2.tgz
# install findspark

import os
os.environ["JAVA_HOME"]="/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"]="/content/spark-3.5.4-bin-hadoop3"
import sys

import time
#ajouter time execution pour chaque code!!!!!!!!!!!!!!!!1

!apt install unzip
!pip install gdown
!gdown "1qr-SBzlojgxzXu2fJx0xaWH9P6vrDpnS&confirm=t"
!unzip -u pagecounts-20160101-000000_parsed.out.zip
!rm pagecounts-20160101-000000_parsed.out.zip

!pip install -q findspark

import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
print(pyspark.__version__)

spark = SparkSession.builder.master("local[*]").appName("LogAnalytics").getOrCreate()
sc=spark.sparkContext

from  pyspark import SparkFiles

spark.sparkContext.addFile("pagecounts-20160101-000000_parsed.out")

pagecount_rdd=sc.textFile(SparkFiles.get("pagecounts-20160101-000000_parsed.out"))
pagecount_rdd.take(20)

pagecounts_rdd_fields = pagecount_rdd.map(lambda l:l.split(' '))
pagecounts_rdd_fields.cache
pagecounts_rdd_fields.take(20)
type(pagecounts_rdd_fields)

#create a table with columns
pagecounts_df = pagecounts_rdd_fields.toDF(["Project","PageTitle","PageHits","PageSize"])
pagecounts_df.show(20)
display(pagecounts_df)
type(pagecounts_df)

from collections import namedtuple

Log=namedtuple("Log", ["Project","PageTitle","PageHits","PageSize"])

def stringToLog(s):
  c=s.split(" ")
  return Log(c[0],c[1],int(c[2]), int(c[3]))

Log_rdd=pagecount_rdd.map(stringToLog)
Log_rdd.cache
type(Log_rdd)
Log_rdd.take(10)

Log_rdd.map(lambda x: x).take(1)

#access the field "project" of the first Log
 # Log_rdd.map(lambda x: x).take(1)[0].Project
 #access the field "project" of the first Log
Log_rdd.map(lambda x: x.Project).take(1)[0]

Log_rdd.take(20)

def print_record(p) :
  if type(p)==list:
    for pp in p : print_record(pp)
  else :
    print("Project code : " +p.Project + "\t PageTitle : " +p.PageTitle+"\t PageHits: "+str(p.PageHits)+"\t PageSize: "+str(p.PageSize))

print_record(Log_rdd.take(20))

print("Total number of records in the dataset is : "+ str(Log_rdd.count()))

print("Min Page Size  : " +str(Log_rdd.map(lambda x: x.PageSize).min()))

print("Max Page Size : " +str(Log_rdd.map(lambda x: x.PageSize).max()))

print("Average Page Size : " +str(Log_rdd.map(lambda x: x.PageSize).mean()))

maxPageSize=Log_rdd.map(lambda x: x.PageSize).max()
print_record(Log_rdd.filter(lambda x :x.PageSize==maxPageSize).take(20))

AverageofPageSize=Log_rdd.map(lambda x: x.PageSize).mean()
print_record(Log_rdd.filter(lambda x :x.PageSize>AverageofPageSize).take(20))

hits_popular_nb=Log_rdd.map(lambda x: (x.PageHits,x)).sortByKey(False).map(lambda x:x[1]).take(10)
print_record(hits_popular_nb)

project_popular_pg=Log_rdd.map(lambda x: (x.Project,x.PageHits)).reduceByKey(lambda x,y: (x+y))\
.map(lambda x :(x[1],x[0])).sortByKey(False).map(lambda x :(x[1],x[0]))
project_popular_pg.take(5)

import string
def clean(str):
  return str.translate(str.maketrans(string.punctuation,'_'*len(string.punctuation))).lower()

uniqueword=Log_rdd.map(lambda x : clean(x.PageTitle))\
.filter(lambda x : not any(char.isdigit() for char in x))\
.flatMap(lambda x : x.split("_")).filter(lambda x :x !="").distinct()
uniqueword.take(10)

word=Log_rdd.map(lambda x : clean(x.PageTitle)).filter(lambda x : not any(char.isdigit() for char in x))\
.flatMap(lambda x : x.split("_")).filter(lambda x :x !="")
word_freq=word.map(lambda x : (x,1)).reduceByKey(lambda x,y :x+y)\
.map(lambda x :(x[1],x[0])).sortByKey(False).map(lambda x :(x[1],x[0]))
word_freq.take(1)

from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, Row

Log_tuple=[StructField("Project", StringType(),True),StructField("PageTitle",StringType(),True),StructField("PageHits",LongType(),True),StructField("PageSize",LongType(),True)]
Log_schema = StructType(Log_tuple)
display(Log_schema)

Log_df = spark.createDataFrame(Log_rdd,Log_schema)
Log_df.show(10)

Log_df.persist()
Log_df.createOrReplaceTempView("wikimedia")

import time

time_start=time.time()
querry3="SELECT max(PageSize) AS MAX, min(PageSize) AS MIN, avg(PageSize) AS AVG FROM Wikimedia"
spark.sql(querry3).show(truncate=False)
print(time.time()-time_start)

time_start=time.time()
querry5="SELECT * FROM Wikimedia WHERE PageHits==(SELECT MAX(PageHits) FROM Wikimedia)"
spark.sql(querry5).show(truncate=False)
print(time.time()-time_start)

time_start=time.time()
querry6="SELECT * FROM Wikimedia WHERE PageSize>(SELECT AVG(PageSize) FROM Wikimedia) "
spark.sql(querry6).show(truncate=False)
print(time.time()-time_start)

query = "SELECT * FROM wikimedia ORDER BY PageHits DESC LIMIT 10"

start_time = time.time()

spark.sql(query).show()

print("Execution time: ", (time.time() - start_time))

query = "SELECT Project, SUM(PageHits) AS TotalPageHits FROM wikimedia GROUP BY Project ORDER BY TotalPageHits DESC LIMIT 5"

start_time = time.time()

spark.sql(query).show()

print("Execution time: ", (time.time() - start_time))
