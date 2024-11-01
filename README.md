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
---
Learning to better position !!!