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

#dd=fread('hdfs://localhost:9000/user/hive/warehouse/aa3','csv')
#dd.show(10)


# Writes data to hdfs with different formats
def fwrite(df,fpath,format,mode='Overwrite',delim=',',head=True):
	if format=='csv':
		df.write.mode(mode).options(header=head, delimiter=delim).csv(fpath)
	elif format=='parquet':
		df.write.mode(mode).parquet(fpath)
	elif format=='json':
		df.write.mode(mode).json(fpath)

#fwrite(df,'hdfs://localhost:9000/user/hive/warehouse/aa3','csv')


# Reads data from Hive table  # We can enhance code to read data from diff DBs
def tread(tname):
	df=getSpark().table(tname)
	return df
#tread("aa").show(10)
   
# Writes data to Hive tables, it can support partition by   
def twrite(df,tname,SaveMode='Overwrite',partitionCol=''):
	if partitionCol=='':
		df.write.mode(SaveMode).saveAsTable(tname)
	elif partitionCol !='':
		df.write.mode(SaveMode).partitionBy(partitionCol).saveAsTable(tname)
#twrite(df,"emppy")


# Drops table from Hive
def tdrop(tname):
	table_name="drop table if exists "+tname
	getSpark().sql(table_name)
#tdrop("fb")
	
# Updates data in table from DF, need more implementation and testing for large volume of data
def tupdate(df,tname,pk_list):
	tdf=tread(tname)
	new_df=tdf.join(df3,pk_list,"left").filter(df3.pk_list.isNull()).union(df)
	new_df.write.mode("Overwrite").saveAsTable(tname)

#twrite(df,"updtable")
#tupdate(df3,"updtable","firstname")
#tdf=tread("updtable")
pk_list="firstname"

#tread("updtable").show(10)
#+---------+----------+--------+----------+------+------+
#|firstname|middlename|lastname|       dob|gender|salary|
#+---------+----------+--------+----------+------+------+
#|   Robert|          |Williams|1978-09-05|     M|  4000|
#|    Maria|      Anne|   Jones|1967-12-01|     F|  4000|
#|      Jen|      Mary|   Brown|1980-02-17|     F|    -1|
#|    James|          |   Smith|1991-04-01|     M|  3000|
#|  Michael|      Rose|        |2000-05-19|     M|  4000|
#+---------+----------+--------+----------+------+------+

#df3.show(10)
#+---------+----------+--------+----------+------+------+
#|firstname|middlename|lastname|       dob|gender|salary|
#+---------+----------+--------+----------+------+------+
#|    James|          |   Smith|1991-04-01|     M|  1000|
#|  Michael|      Rose|        |2000-05-19|     M|  1000|
#|   Robert|          |Williams|1978-09-05|     M|  1000|
#|    Maria|      Anne|   Jones|1967-12-01|     F|  1000|
#|      Jen|      Mary|   Brown|1980-02-17|     F|  1000|
#+---------+----------+--------+----------+------+------+
