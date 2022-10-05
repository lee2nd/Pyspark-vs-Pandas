# Pyspark
We don't approve to upload the local programming language, 
like Python, to the cloud by copy-paste directly, 
because the computing efficiency would be lower than Pyspark. 
And parallel computing is an advantage on the cloud used by Pyspark. 
The following will compare common data wrangling methods in Pyspark and pandas data frame.

## Chapters

1.  [Load data from local files](#1-load-data-from-local-files)
2.  [Display the schema of the DataFrame](#2-display-the-schema-of-the-dataframe)
3.  [Change data types of the DataFrame](#3-change-data-types-of-the-dataframe)
4.  [Show the head of the DataFrame](#4-show-the-head-of-the-dataframe)
5.  [Select columns from the DataFrame](#5-select-columns-from-the-dataframe)
6.  [Show the statistics of the DataFrame](#6-show-the-statistics-of-the-dataframe)
7.  [Drop duplicates](#7-drop-duplicates)
8.  [Missing Values (check NA, drop NA, replace NA)](#8-missing-values-(check-na-drop-na-replace-na))
9.  [Datetime manipulations](#datetime-manipulations)
10.  [Filter data based on conditions](#filter-data-based-on-conditions) 
11.  [Group By with Aggregation Functions](#11-group-by-with-aggregation-functions)
12.  [Sort Data](#12-sort-data)
13.  [Rename columns](#13-rename-columns)
14.  [Create a new column](#14-create-a-new-column) 
15.  [Join tables](#15-join-tables) 
16.  [User Defined functions](#16-user-defined-functions) 
17.  [Window Function to calculate](#17-window-function-to-calculate) 
18.  [Operate SQL queries within DataFrame](#18-operate-sql-queries-within-dataframe) 
19.  [Convert one type of DataFrame to another](#19-convert-one-type-of-dataframe-to-another)
20.  [Export the Data](#20-export-the-data) 
21.  [Converting a PySpark DataFrame Column to a Python List](#21-converting-a-pyspark-dataframe-column-to-a-python-list)



## Libraries and Settings


+ libraries

        import json
        import pyspark
        import random
        import pandas as pd 
        import numpy as np
        import pyspark.sql.functions as F
        from pyspark.sql import Window
        from pyspark.sql.functions import udf, desc, asc, isnan, when, count, col
        from pyspark.sql.types import StructField,IntegerType, StructType,StringType, FloatType, DateType


+ settings

        notebook_info = json.loads(dbutils.notebook \
                            .entry_point\ 
                            .getDbutils()\
                            .notebook()\
                            .getContext()\
                            .toJson())
                            
        user = notebook_info['tags']['user']
        
        local_file = f'/dbfs/FileStore/{user}/us_counties.csv'
        
        dbfs_path = f"dbfs:/FileStore/{user}/"
        
        dbfs_file = f"{dbfs_path}us_counties.csv"



## 1. Load data from local files

+ Pandas        

      df = pd.read_csv(local_file)

  
+ Pyspark   

      df_s = spark.read.csv(dbfs_file, header = True)



## 2. Display the schema of the DataFrame

+ pandas      

      print(df.dtypes)
 
+ pyspark       
  
      print(df_s.printSchema())



## 3. Change data types of the DataFrame

+ pandas : aim to reduce memory size 

      types = {'date': 'object', 'county': 'object', 
               'state': 'object', 'fips': 'float',
               'cases': pd.Int64Dtype(), 'deaths': pd.Int64Dtype()}
      df = pd.read_csv(local_file, usecols=types.keys(), dtype=types)

+ pyspark 

      newDF=[StructField('date',StringType(),True),
             StructField('county',StringType(),True),
             StructField('state',StringType(),True),
             StructField('fips',FloatType(),True),
             StructField('cases',IntegerType(),True),
             StructField('deaths',IntegerType(),True)]
      finalStruct=StructType(fields=newDF)
      df_s = spark.read.csv(dbfs_file,header = True,schema=finalStruct)
      df_s.printSchema()



## 4. Show the head of the DataFrame

+ pandas 

      df.head()

  
+ pyspark:\
    In pyspark, take() and show() are different. show() prints results, take() returns a list of rows (in PySpark) and can be used to create a new dataframe. They are both actions.   

      df_s.show(5)
      df_s.take(5)
      display(df_s)




## 5. Select columns from the DataFrame

+ Pandas 

      df[['state','cases']].head()

  
+ pyspark 

      df_s.select('state','cases').show(5)



## 6. Show the statistics of the DataFrame

+ pandas      

      df.describe()               # show the stats for all numerical columns 
      df['cases'].describe()      # show the stats for a specific column

+ pyspark 

      df_s.describe().show()             #pyspark can show stats for all columns but it may contain missing values 
      df_s.describe('cases').show()      # show the stats for a specific column



## 7. Drop duplicates

+ pandas

      df.state.drop_duplicates().head()

+ pyspark 

      df_s.select('state').dropDuplicates().show(5)




## 8. Missing Values (check NA, drop NA, replace NA)

+ Pandas 
  

      ## Check NA
      # if we want to check the non-missing value we can use notnull() instead
      df.isnull().sum() #check the number of missing value for each column
      
      ## Drop NA
      df_valid = df.dropna(subset=['fips','state'], how ='any')     # all 
      print(df_valid.shape[0])
      print(df.shape[0])
          
      ## Replace NA
      print(df.fillna(0).isnull().sum())            # Replace Missing values with 0 
      values = {'fips': -1, 'cases': 0, 'deaths': 0}
      df.fillna(value=values, inplace = True)       # Replace Missing values based on specific columns 
  
+ pyspark  
 
      ## Check NA
      # if we want to check the non-missing value we can use isNotNull()
      df_s.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_s.columns]).show()      

  
      ## Drop NA
      df_s_valid = df_s.dropna(how='any', subset =['fips', 'state'])      # all
      print(df_s_valid.count())
      print(df_s.count())
        
      ## Replace NA
      df_s.na.fill(0).select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_s.columns]).show()      
        # Replace Missing values with 0 
      df_s.fillna({'fips':'0','cases': 0, 'deaths': 0 })      # Replace Missing values based on specific columns


## 9. Datetime manipulations

+ pandas  
  
      df['date'] = pd.to_datetime(df.date)      # change the data type from string to datetime 
      df.date.dt.weekday.head()                 
       # Extract weekday from the datetime column,alternatively, we can use year, month, day,dayofweek ,second,hour, quarter instead 

+ pyspark 
  
      df_spark = df_s.withColumn("record_date",df_s['date'].cast(DateType()))      # change the data type from string to datetime 
      df_spark.printSchema() #show the current schema 
      newdf = df_spark.select(year(df_spark.record_date).alias('dt_year'), 
                              month(df_spark.record_date).alias('dt_month'),
                              dayofmonth(df_spark.record_date).alias('dt_day'), 
                              dayofyear(df_spark.record_date).alias('dt_dayofy'),   
                              hour(df_spark.record_date).alias('dt_hour'), 
                              minute(df_spark.record_date).alias('dt_min'),
                              weekofyear(df_spark.record_date).alias('dt_week_no'),
                              unix_timestamp(df_spark.record_date).alias('dt_int'))
      newdf.show()



## 10. Filter data based on conditions

+ Pandas  
    
      print(df[df.state =='California'].head())      # Filter data based on the state is CA
      print(df[df.state =='California'] [['cases','deaths']].head()) 
        # only show specific columns based on the filtering condistions

+ pyspark   
  
      df_s.where(df_s.state == 'California').show(5)
      df_s[df_s.state.isin("California")].show(5)      # Alternatively, we can write it in this way
      df_s.filter(df_s.state == 'California').select('deaths', 'cases').show(5)   
        # only show specific columns based on the filtering condistions 



## 11. Group By with Aggregation Functions

+ pandas 
     
      df.groupby('state').cases.sum() # the total cases for each state, can change to min(),max(), mean()
      df.groupby(['state','date']).agg({'cases': 'sum', 'deaths': 'max'}).head()      
        # apply different aggregation functions for different columns

+ pyspark   
 
      df_s.groupBy(df_s.state).agg(F.sum('cases')).show()      # F.mean(), F.max(), F.countDistinct(), F.min(), F.count()
      df_s.groupBy(['state','date']).agg({'cases': 'sum', 'deaths': 'max'}).show()     
        # apply different aggregation functions for different columns



## 12. Sort Data

+ pandas  
    
      df_agg = df.groupby(['state','date']).agg({'deaths': 'sum',
                                                   'cases': 'sum'}).reset_index().sort_values(by =['state', 'date'],
                                                                                              ascending = True)
      df_agg.head(10)

+ spark   
  
      df_s_agg = df_s.groupBy(['state','date']).agg({'deaths': 'sum', 
                                                       'cases': 'sum'}).sort(['state','date'], ascending =True)
      df_s_agg.show()



## 13. Rename columns

+ pandas 
 
      df_agg.rename(columns={"deaths": "total_death", "cases": "total_cases"}, inplace = True)
      df_agg.head()

+ pyspark  
   
      df_s_agg = df_s_agg.withColumnRenamed("sum(cases)","total_cases").withColumnRenamed("sum(deaths)", "total_death")
      df_s_agg.show(5) 



## 14. Create a new column 

+ pandas
  
      df_agg['fata_rate'] = df_agg['total_death']/df_agg['total_cases'] 
        # show the fatality rate, create a new column by dividing two columns 
      df_agg.sort_values(by='fata_rate',ascending = False).head(10)
  
+ pyspark

      df_s_agg = df_s_agg.withColumn('fata_rate', F.col('total_death')/F.col('total_cases')).sort('fata_rate',ascending =False)        # use withColumn to create a new column in pyspark
      df_s_agg.show(10)



## 15. Join tables

+ pandas 
       
      table_a = df.groupby('fips').cases.sum().reset_index()      
        # create a new dataframe by calculating the total case for each fips 
      table_a.columns = ['fips', 'total_cases']      # rename the columns for the new dataframe
      table_b = df[['county','state','fips']]      
        # create another dataframe by only extracting some columns from the original data frame 
      table_b.merge(table_a , on ='fips', how ='left').head()      # Left Join two tables 

+ pyspark 
        
      TableA =  df_s.groupBy('fips').agg(F.sum('cases').alias('total_cases'))
      TableB = df_s.select(['county','state','fips'])
      ta = TableA.alias('ta')      
        # The alias provides a short name for referencing fields and for referencing the fields after creation of the joined table.
      tb = TableB.alias('tb')      
        # The alias provides a short name for referencing fields and for referencing the fields after creation of the joined table.
      tb.join(ta, ta.fips==tb.fips, how ='left' ).select('county', 'state', 'tb.fips', 'total_cases').show(5) 
        # Could also use 'left_outer', 'right', 'right_outer', 'full', default ‘inner’



## 16. User Defined functions

+ pandas 

      df['new_num']= df.cases.apply(lambda x: x+3)
      df.head()

  
+ pyspark 

       add_number = udf(lambda x: x+3)
       df_s = df_s.withColumn("new_num", add_number(df_s.cases))
       df_s.show(5)



## 17. Window Function to calculate

Window function can be one of any of the standard aggregate functions (sum, count, max, min, avg) as well as a number of functions that can only be used as analytic functions. Window function can help us to write a query with less complexity. It's mainly composed of four main parts:
1. The over() caluse: This tells the databaseto expect a window function, rather than a standard aggregate function.
2. The partitionBy() clause: This clause tells the database how to break up the data. In other words, it is similar to a Group By in that it tells the database that row with the same values should be treated as a single group or parition.
3. The orderBy() clause: It tells the database how to sort the data within each partition.
4. The rangeBetween() clause: It defines the regions over which the function is calculated. 
      - unboundedPreceding: from the start of the partition 
      - unboundedFollowing: to the end of the partition 
      - 0: current row 
        
+ pandas 

      df_agg.groupby(['state', 'date']).sum().groupby(level=0).cumsum().reset_index()

+ pyspark     

      w = Window \
            .partitionBy('state') \
            .orderBy(asc('date')) \
            .rangeBetween(Window.unboundedPreceding, 0)
      df2 = df_s_agg.withColumn('cumulative_cases', 
                                  F.sum(df_s_agg.total_cases).over(w))\
                                  .withColumn('cumulative_deaths',F.sum(df_s_agg.total_death).over(w))\
                                  .select(['state','date','total_cases','cumulative_cases', 'cumulative_deaths'])\
                                  .orderBy('state')
      df2.show()



## 18. Operate SQL queries within DataFrame

+ pandas    
  
      df.query("state=='California'").count()      # query is to query the columns of a frame with a boolean expression.

+ pyspark 
   
      df_s.createOrReplaceTempView("log_table")
      spark.sql("select count(1) from log_table where state ='California'").show()



## 19. Convert one type of DataFrame to another

+ pandas to pyspark    
 
      df_s_agg_2 = spark.createDataFrame(df_agg)
      df_s_agg_2.show(5)

+ pyspark to pandas       
  
      df_s_agg.toPandas().head()



## 20. Export the Data

+ pandas 

      df_agg.to_csv('pd.csv')      # Save the data as csv file 

+ pyspark   
      
      df_s_agg.write.csv('pyspark2.csv',header ='true')      
        # alternatively we can save pyspark to csv in this way if Spark 2.0+
      df_s_agg.write.format('com.databricks.spark.csv').save("pyspark.csv",header = 'true')      # Save the data as csv file




## 21. Converting a PySpark DataFrame Column to a Python List

+ pandas  
 
      #df.values.tolist()
      list(df['date'])
      df['date'].tolist()

+ pyspark

      ps_list = list(df_s.select('date').toPandas()['date']) 
      ps_list = df_s.select('date').rdd.map(lambda row : row[0]).collect()
      print(ps_list)


[Top](#pyspark)
