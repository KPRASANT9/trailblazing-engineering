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
	
# Updates data in table from DF
def tupsert(df,tname,pk_list):
	tdf=tread(tname)
	tmp_tbl=tname+"_tmp"
	upd_df=tdf.join(df,pk_list,"leftanti").union(df)
	twrite(upd_df,tmp_tbl)
	df_tmp=tread(tmp_tbl)
	twrite(df_tmp,tname)
	tdrop(tmp_tbl)
	return None

#pk_list["emp_id","dept_id"]
#tupsert(df,tname,pk_list)

#buidls join condition
def join_key(pk_list):
	s =[]
	for item in pk_list:
   		s.append('tdf.'+item+'='+'df.'+item)
	jk=' && '.join(s)
	return jk
#pk_list = ['id', 'name'] -->tdf.id=df.id && tdf.name=df.name
