print("Hello Sathya !!!")

#Reading file for column rename and make Key value pair
f="/home/ag68920/colc.txt"
rename = {k:v for k, v in (l.rstrip('\n').split('=') for l in open(f))}
#rename = {'name': 'fname', 'sno': 'fsno'}

df = spark.createDataFrame([("sathya", 1), ("Myra", 2)],["name", "sno"])
for col in df.schema.names:
    df = df.withColumnRenamed(col, rename[col])


#selecting only required columns for Target
df2=spark.table(targetTable)
reqColumns =df2.columns
df.toDF(*reqColumns).printSchema()


#an empty dictionary
f="/home/ag68920/dic.txt"
dictionary = {}
with open(f) as file:
 for line in file:
    (key, value) = line.rstrip('\n').split("=")
    dictionary[key] = str(value)
 
print ('\ntext file to dictionary=\n',dictionary)

############################################################
JSON


import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
val df = spark.read.option("multiline",true).json("hdfs://nameservicets1/tmp/complex.json")

val explodedDF = df.select($"dc_id", explode($"source"))
explodedDF.printSchema()
explodedDF.show(false)


 val explodedDF = df.select("dc_id", "source.*")

############################################################



from pyspark.sql.functions import explode
from pyspark.sql.functions import col
def flatten_df(nested_df):
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']
    flat_df = nested_df.select(flat_cols + [col(nc+'.'+c).alias(nc+'_'+c) for nc in nested_cols for c in nested_df.select(nc+'.*').columns])
    if not nested_cols: final_df=flat_df
    else: final_df=flatten_df(flat_df)
    return final_df
    

def flatten_df(nested_df):
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']
    if not nested_cols: 
        return nested_df
    else: 
        flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
        flat_df=nested_df.select(flat_cols + [col(nc+'.'+c).alias(nc+'_'+c) for nc in nested_cols for c in nested_df.select(nc+'.*').columns])
        return flatten_df(flat_df)
    
        
        
nested_df=df2   
xx=flatten_df(df2)
xx2=flatten_df(xx)
xx3=flatten_df(xx2)

y5=flatten_df(df2)
y5.printSchema()
y5.show()


y3=flatten_df(y2)
y3.printSchema()
y3.show()




data= [(""" {"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"} """)]

############################################################
Final

from pyspark.sql.functions import explode
from pyspark.sql.functions import col
def flatten_df(nested_df):
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']
    if not nested_cols:
        arr_cols = [c[0] for c in nested_df.dtypes if c[1][:5] == 'array']
        if not arr_cols:
            return nested_df
        else:
            for x in arr_cols:
               nested_df = nested_df.withColumn(x, explode(col(x)))
            return nested_df
    else: 
        flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
        flat_df=nested_df.select(flat_cols + [col(nc+'.'+c).alias(nc+'_'+c) for nc in nested_cols for c in nested_df.select(nc+'.*').columns])
        return flatten_df(flat_df)

df5=flatten_df(df2)
df5.printSchema()
df5.show()


cols = [c[0] for c in y5.dtypes if c[1][:5] == 'array']
def explode_cols(df, cols):
    for x in cols:
        e_df = df.withColumn(x, explode(col(x)))
    return e_df
result = explode_cols(y5, cols)



+-------+------+-----------------------+------------------------------+--------------------------------+-----------------------+-----------------------+-------------------------+-----------------------------+-------------------------------+----------------------+----------------------+------------------------+----------------------+----------------------------+------------------------------+---------------------+---------------------+-----------------------+------------------------------+--------------------------------+-----------------------+-----------------------+-------------------------+----------------------------+-----------------------------+---------------------------+----------------------------+--------------------------+---------------------------+----------------------------+-----------------------------+
|  dc_cc| dc_id|source_sensor-igauge_ar|source_sensor-igauge_c02_level|source_sensor-igauge_description|source_sensor-igauge_id|source_sensor-igauge_ip|source_sensor-igauge_temp|source_sensor-inest_c02_level|source_sensor-inest_description|source_sensor-inest_id|source_sensor-inest_ip|source_sensor-inest_temp|source_sensor-ipad_ar2|source_sensor-ipad_c02_level|source_sensor-ipad_description|source_sensor-ipad_id|source_sensor-ipad_ip|source_sensor-ipad_temp|source_sensor-istick_c02_level|source_sensor-istick_description|source_sensor-istick_id|source_sensor-istick_ip|source_sensor-istick_temp|source_sensor-igauge_geo_lat|source_sensor-igauge_geo_long|source_sensor-inest_geo_lat|source_sensor-inest_geo_long|source_sensor-ipad_geo_lat|source_sensor-ipad_geo_long|source_sensor-istick_geo_lat|source_sensor-istick_geo_long|
+-------+------+-----------------------+------------------------------+--------------------------------+-----------------------+-----------------------+-------------------------+-----------------------------+-------------------------------+----------------------+----------------------+------------------------+----------------------+----------------------------+------------------------------+---------------------+---------------------+-----------------------+------------------------------+--------------------------------+-----------------------+-----------------------+-------------------------+----------------------------+-----------------------------+---------------------------+----------------------------+--------------------------+---------------------------+----------------------------+-----------------------------+
|dcc-102|dc-101|                      1|                          1475|            Sensor attached t...|                     10|            68.28.91.22|    35|                         1346|           Sensor attached t...|                     8|       208.109.163.218|                      40|                     4|                1370|          Sensor ipad attac...|                   13|          67.185.72.1|                     34|                          1574|            Sensor embedded i...|                      5|         204.116.105.67|                       40|                        38.0|                         97.0|                      33.61|                     -111.89|                     47.41|                     -122.0|                       35.93|                       -85.46|
|dcc-102|dc-101|                      1|                          1475|            Sensor attached t...|                     10|            68.28.91.22|    35|                         1346|           Sensor attached t...|                     8|       208.109.163.218|                      40|                     5|                1370|          Sensor ipad attac...|                   13|          67.185.72.1|                     34|                          1574|            Sensor embedded i...|                      5|         204.116.105.67|                       40|                        38.0|                         97.0|                      33.61|                     -111.89|                     47.41|                     -122.0|                       35.93|                       -85.46|
|dcc-102|dc-101|                      1|                          1475|            Sensor attached t...|                     10|            68.28.91.22|    35|                         1346|           Sensor attached t...|                     8|       208.109.163.218|                      40|                     6|                1370|          Sensor ipad attac...|                   13|          67.185.72.1|                     34|                          1574|            Sensor embedded i...|                      5|         204.116.105.67|                       40|                        38.0|                         97.0|                      33.61|                     -111.89|                     47.41|                     -122.0|                       35.93|                       -85.46|
|dcc-102|dc-101|                      2|                          1475|            Sensor attached t...|                     10|            68.28.91.22|    35|                         1346|           Sensor attached t...|                     8|       208.109.163.218|                      40|                     4|                1370|          Sensor ipad attac...|                   13|          67.185.72.1|                     34|                          1574|            Sensor embedded i...|                      5|         204.116.105.67|                       40|                        38.0|                         97.0|                      33.61|                     -111.89|                     47.41|                     -122.0|                       35.93|                       -85.46|
|dcc-102|dc-101|                      2|                          1475|            Sensor attached t...|                     10|            68.28.91.22|    35|                         1346|           Sensor attached t...|                     8|       208.109.163.218|                      40|                     5|                1370|          Sensor ipad attac...|                   13|          67.185.72.1|                     34|                          1574|            Sensor embedded i...|                      5|         204.116.105.67|                       40|                        38.0|                         97.0|                      33.61|                     -111.89|                     47.41|                     -122.0|                       35.93|                       -85.46|
|dcc-102|dc-101|                      2|                          1475|            Sensor attached t...|                     10|            68.28.91.22|    35|                         1346|           Sensor attached t...|                     8|       208.109.163.218|                      40|                     6|                1370|          Sensor ipad attac...|                   13|          67.185.72.1|                     34|                          1574|            Sensor embedded i...|                      5|         204.116.105.67|                       40|                        38.0|                         97.0|                      33.61|                     -111.89|                     47.41|                     -122.0|                       35.93|                       -85.46|
|dcc-102|dc-101|                      3|                          1475|            Sensor attached t...|                     10|            68.28.91.22|    35|                         1346|           Sensor attached t...|                     8|       208.109.163.218|                      40|                     4|                1370|          Sensor ipad attac...|                   13|          67.185.72.1|                     34|                          1574|            Sensor embedded i...|                      5|         204.116.105.67|                       40|                        38.0|                         97.0|                      33.61|                     -111.89|                     47.41|                     -122.0|                       35.93|                       -85.46|
|dcc-102|dc-101|                      3|                          1475|            Sensor attached t...|                     10|            68.28.91.22|    35|                         1346|           Sensor attached t...|                     8|       208.109.163.218|                      40|                     5|                1370|          Sensor ipad attac...|                   13|          67.185.72.1|                     34|                          1574|            Sensor embedded i...|                      5|         204.116.105.67|                       40|                        38.0|                         97.0|                      33.61|                     -111.89|                     47.41|                     -122.0|                       35.93|                       -85.46|
|dcc-102|dc-101|                      3|                          1475|            Sensor attached t...|                     10|            68.28.91.22|    35|                         1346|           Sensor attached t...|                     8|       208.109.163.218|                      40|                     6|                1370|          Sensor ipad attac...|                   13|          67.185.72.1|                     34|                          1574|            Sensor embedded i...|                      5|         204.116.105.67|                       40|                        38.0|                         97.0|                      33.61|                     -111.89|                     47.41|                     -122.0|                       35.93|                       -85.46|
+-------+------+-----------------------+------------------------------+--------------------------------+-----------------------+-----------------------+-------------------------+-----------------------------+-------------------------------+----------------------+----------------------+------------------------+----------------------+----------------------------+------------------------------+---------------------+---------------------+-----------------------+------------------------------+--------------------------------+-----------------------+-----------------------+-------------------------+----------------------------+-----------------------------+---------------------------+----------------------------+--------------------------+---------------------------+----------------------------+-----------------------------+


x=[("""{
"dc_cc": "dcc-102",
"dc_id": "dc-101",
"source": {
    "sensor-igauge": {
      "id": 10,
      "ip": "68.28.91.22",
      "description": "Sensor attached to the container ceilings",
      "temp":35,
      "c02_level": 1475,
      "geo": {"lat":38.00, "long":97.00},  
      "ar": [1,2,3]
    },
    "sensor-ipad": {
      "id": 13,
      "ip": "67.185.72.1",
      "description": "Sensor ipad attached to carbon cylinders",
      "temp": 34,
      "c02_level": 1370,
      "geo": {"lat":47.41, "long":-122.00},
      "ar2": [4,5,6]
    },
    "sensor-inest": {
      "id": 8,
      "ip": "208.109.163.218",
      "description": "Sensor attached to the factory ceilings",
      "temp": 40,
      "c02_level": 1346,
      "geo": {"lat":33.61, "long":-111.89}
    },
    "sensor-istick": {
      "id": 5,
      "ip": "204.116.105.67",
      "description": "Sensor embedded in exhaust pipes in the ceilings",
      "temp": 40,
      "c02_level": 1574,
      "geo": {"lat":35.93, "long":-85.46}
    }
  }
}""")]

rdd = spark.sparkContext.parallelize(x)
df2 = spark.read.json(rdd)
df2.show()
df2.printSchema()


############################################################
GIT version
#Reading file for column rename and make Key value pair
def col_rename(df, f):
    col_map = {k:v for k, v in (l.rstrip('\n').split('=') for l in open(f))}
    for col in df.schema.names:
        new_df = df.withColumnRenamed(col, col_map[col])
    return new_df

#	col_rename(df,f)

#selecting only required columns for Target
def match_col(df, t_tbl):
    t_df=spark.table(t_tbl)
    reqColumns =t_df.columns
    new_df=df.toDF(*reqColumns)
	return new_df

#match_col(df,tbl)


#JSON_PARSE, STRUCT and MAP flatten
df2 = spark.read.json(complex.json)
df2.printSchema()

from pyspark.sql.functions import explode
from pyspark.sql.functions import col
def flatten_df(nested_df):
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']
    if not nested_cols:
        arr_cols = [c[0] for c in nested_df.dtypes if c[1][:5] == 'array']
        if not arr_cols:
            return nested_df
        else:
            for x in arr_cols:
               nested_df = nested_df.withColumn(x, explode(col(x)))
            return nested_df
    else: 
        flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
        flat_df=nested_df.select(flat_cols + [col(nc+'.'+c).alias(nc+'_'+c) for nc in nested_cols for c in nested_df.select(nc+'.*').columns])
        return flatten_df(flat_df)

df5=flatten_df(df2)
df5.printSchema()
df5.show()	


