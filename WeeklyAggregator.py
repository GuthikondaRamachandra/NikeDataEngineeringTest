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
datewithmonthpath = '/FileStore/tables/calendar_with_months-1.csv'
print(datespath)
print(productpath)
print(salespath)
print(storepath)

datedf = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(datewithmonthpath)
#datedf.show()
#4965


#datedf=datedf.withColumn("salesUnits",col("salesUnits"))

productdf = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(productpath)
#productdf.show()
salesdf = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(salespath)
#salesdf.show()
storedf = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(storepath)
#storedf.show()
df1=salesdf.join(datedf,salesdf.dateId == datedf.datekey,"left").drop(datedf.datekey).drop(salesdf.dateId)
#df1.show()
#df1.count()
df2=df1.join(productdf,df1.productId == productdf.productid,"left").drop(productdf.productid)
#df2.show()
#df2.count()
df3=df2.join(storedf,df2.storeId == storedf.storeid,"left").drop(storedf.storeid)
#display(df3)
#df3.count()
#finaldf=df3.groupBy(df3.datecalendaryear,df3.weeknumberofseason,df3.netSales,df3.salesUnits).agg(df3.netSales,df3.salesUnits)
df3=df3.withColumn("salesUnits",col("salesUnits").cast("int")).withColumn("netSales",col("netSales").cast("double"))
finaldf=df3.groupBy("division","gender","category","channel","datecalendaryear","WeekNumberoftheYear").sum("salesUnits","netSales")
finaldf=finaldf.withColumnRenamed("sum(salesUnits)","SalesUnits").withColumnRenamed("sum(netSales)","NetSales")
finaldf=finaldf.withColumn("year",concat(lit("RY"),substring(finaldf.datecalendaryear,3,4)))
#finaldf.show()
finaldf=finaldf.withColumn("UniqueKey",concat(finaldf.year,lit("_"),finaldf.channel,lit("_"),finaldf.division,lit("_"),finaldf.gender,lit("_"),finaldf.category))
finaldf=finaldf.withColumn("SalesUnitsDataRow",concat(lit("{'W"),finaldf.WeekNumberoftheYear,lit("':"),finaldf.SalesUnits,lit("}")))
finaldf=finaldf.withColumn("NetSalesDataRow",concat(lit("{'W"),finaldf.WeekNumberoftheYear,lit("':"),finaldf.NetSales,lit("}")))


  

  #['{"division":"APPAREL","gender":"KIDS","category":"CRICKET","channel":"Digital","datecalendaryear":"2018","WeekNumberoftheYear":"1","year":"RY18",DataRows : [
   #{"SalesUnitsDataRow":"W1:199"},
   #{"NetSalesDataRow":"W1:12009.599999999993"}
   
   #]
   
   #"UniqueKey":"RY18_Digital_APPAREL_KIDS_CRICKET"}
 
#finaldf.show()
finaljson=finaldf.toJSON().collect()
#print(finaljson)

for d in finaljson:
  d1=json.loads(d)
  new_dict={}
  new_list=[]
  #print(d)
  print(type(d1))
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
  print(new_dict)      
    

# COMMAND ----------

dbutils.fs.ls(".")
