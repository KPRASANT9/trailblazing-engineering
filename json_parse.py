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

#df2 = spark.read.json(complex.json)
#df2.printSchema()
#df5=flatten_df(df2)
#df5.printSchema()
#df5.show()	
