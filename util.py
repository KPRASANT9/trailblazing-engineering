from typing import Any
from pyspark.sql import SparkSession

# Create SparkSession
spark_session = SparkSession.builder.master("local[1]").appName("Myspark application").getOrCreate()

def getSpark():
    global spark_session
    return spark_session
	
# Reads different file formats 
def fread(fpath,format,delim=',',head=True):
	spark=getSpark()
	if format=='csv':
		df=spark.read.options(header=head, delimiter=delim).format(format).load(fpath)
	elif format=='parquet':
		df=spark.read.format(format).load(fpath)
	elif format=='json':
		df=spark.read.format(format).load(fpath)
	return df

# Writes data to hdfs with different formats
def fwrite(df,fpath,format,mode='Overwrite',delim=',',head=True):
	if format=='csv':
		df.write.mode(mode).options(header=head, delimiter=delim).csv(fpath)
	elif format=='parquet':
		df.write.mode(mode).parquet(fpath)
	elif format=='json':
		df.write.mode(mode).json(fpath)

# Reads data from Hive table  # We can enhance code to read data from diff DBs
def tread(tname):
	spark=getSpark()
	df=spark.table(tname)
	return df
   
# Writes data to Hive tables, it can support partition by   
def twrite(df,tname,SaveMode='Overwrite',partitionCol=''):
	if partitionCol=='':
		df.write.mode(SaveMode).saveAsTable(tname)
	elif partitionCol !='':
		df.write.mode(SaveMode).partitionBy(partitionCol).saveAsTable(tname)

# Drops table from Hive
def tdrop(tname):
	getSpark.sql(""" DROP TABLE IF EXISTS tname""")
	
# Updates data in table from DF, need more implementation and testing for large volume of data
def tupdate(df,tname,pk_list):
	spark=getSpark()
	tdf=tread(tname)
	new_df=tdf.join(df,pk_list,"left").filter(df.pk_list.isNull).union(df)
	new_df.write.mode("Overwrite").saveAsTable(tname)
