# Databricks notebook source
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit,concat,substring
from pyspark.sql.types import StructType,StringType,StructField,DoubleType,IntegerType
import json


datepath = '/FileStore/tables/calendar.csv'
productpath = '/FileStore/tables/product.csv'
salespath = '/FileStore/tables/sales.csv'
storepath = '/FileStore/tables/store.csv'
datewithmonthpath = '/FileStore/tables/calendar_with_months.csv'
# create output directory
dbutils.fs.mkdirs("/nike_output/")
# reading data stored in Databricks default path
datedf = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(datewithmonthpath)
productdf = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(productpath)
salesdf = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(salespath)
storedf = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(storepath)
#joining the sales table to calendar(date),product,store details
df1=salesdf.join(datedf,salesdf.dateId == datedf.datekey,"left").drop(datedf.datekey).drop(salesdf.dateId)
df2=df1.join(productdf,df1.productId == productdf.productid,"left").drop(productdf.productid)
df3=df2.join(storedf,df2.storeId == storedf.storeid,"left").drop(storedf.storeid)
df3=df3.withColumn("salesUnits",col("salesUnits").cast("int")).withColumn("netSales",col("netSales").cast("double"))
#aggregation 
finaldf=df3.groupBy("division","gender","category","channel","datecalendaryear","WeekNumberoftheYear").sum("salesUnits","netSales")
finaldf=finaldf.withColumnRenamed("sum(salesUnits)","SalesUnits").withColumnRenamed("sum(netSales)","NetSales")
finaldf=finaldf.withColumn("year",concat(lit("RY"),substring(finaldf.datecalendaryear,3,4)))
finaldf=finaldf.withColumn("UniqueKey",concat(finaldf.year,lit("_"),finaldf.channel,lit("_"),finaldf.division,lit("_"),finaldf.gender,lit("_"),finaldf.category))
finaldf=finaldf.withColumn("SalesUnitsDataRow",concat(lit("{\"W"),finaldf.WeekNumberoftheYear,lit("\":"),finaldf.SalesUnits,lit("}")))
finaldf=finaldf.withColumn("NetSalesDataRow",concat(lit("{\"W"),finaldf.WeekNumberoftheYear,lit("\":"),finaldf.NetSales,lit("}")))
# method to create final dictionary for output json
def create_final_dictionary(new_dict):
  for i in range(0,2):
    datarow=new_dict['DataRows'][i]['DataRow']
    print(type(datarow))
    print(datarow)
    datarow_dict=json.loads(datarow)
    full_week_dict={}
    for w in range (0,53):
        k='W'+str(w)
        v=0
        full_week_dict[k]=v

    for k,vv in full_week_dict.items():
        if k in datarow_dict.keys():
            full_week_dict[k]=datarow_dict[k]
    
    new_dict['DataRows'][i]['DataRow']=full_week_dict
  return new_dict

# convert dataframe to json
finaljson=finaldf.toJSON().collect()
final_str=''
for d in finaljson:
  d1=json.loads(d)
  new_dict={}
  new_list=[]
  #print(d)
  #print(type(d1))
  for k,v in d1.items():
    if k=='SalesUnitsDataRow' or  k=='NetSalesDataRow' :
      inner_dict={}
      inner_dict['RowID'] =k
      inner_dict['DataRow'] = v
      if 'Net' in k:
        inner_dict['_comment'] ='Net Sales needs to be aggregated using transaction sales.csv data on weekly basis. If the value is not present for some week it needs to be filled with zero as given in below example'
      if 'Units' in k:
        inner_dict['_comment']='Sales Unit needs to be aggregated using transactional sales.csv data on weekly basis. If the value is not present for some week it needs to be filled with zero as given in below example'
      
      new_list.append(inner_dict)
    else:
      new_dict[k]=v
  new_dict['DataRows']=new_list
  # Final Data
  #print(new_dict)
  final_dict=create_final_dictionary(new_dict)
  print(final_dict)
  final_str=final_str+str(final_dict)  
dbutils.fs.put("/nike_output/consumption.json",final_str)   

# COMMAND ----------

dbutils.fs.head("/nike_output/consumption.json")
