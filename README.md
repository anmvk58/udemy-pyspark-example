## Example for SparkSQL vs Spark DataFrame
---
### Guide: 
1) Link to get Data: [Data](https://data.sfgov.org/Public-Safety/Fire-Department-Calls-For-Service-2016-/kikm-y2iv/about_data)

2) Add this line to spark-defaults.conf for recognize log4j config: 
```
spark.driver.extraJavaOptions -Dlog4j.configuration=file:../log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=hello-spark
```

3) For read/write avro:
```
spark.jars.packages  org.apache.spark:spark-avro_2.12:3.5.3
```
---
### Project Structure
- __1_sql_and_dataframe__: example for processing data by 2 way: Spark SQL and Spark Dataframe 
- __2_schema_reader__: example for making schema and apply it into dataframe
- __3_dataframe_writer__: example for writer/sink data
- __4_spark_sql_table__: example for create spark sql Database and Tables
- __5_dataframe_row__: example and work with Dataframe Rows and Unit Test
- __6_mining_log_files__: example for mining log file 
- __7_exploring_columns__: example for using columns by 2 ways 
- __8_user_define_functions__: example for define your custom function 
- __9_misc_columns__: misc in process column 
- __10_aggregate__: demo aggregate function
- __11_grouping__: demo aggregate when group by ...
- __12_windowing_process__: calculate in a group
- __13_ranking_demo__: ranking result by use window
- __14_join_demo__: demo joining 2 dataframe 
- __15_performance_in_joining__: 2 way to increase performance when join
---
Learning to better position !!!