## Example for SparkSQL vs Spark DataFrame

Link to get Data: [Data](https://data.sfgov.org/Public-Safety/Fire-Department-Calls-For-Service-2016-/kikm-y2iv/about_data)

Notice: Add this line to spark-defaults.conf: 
```
spark.driver.extraJavaOptions -Dlog4j.configuration=file:../log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=hello-spark
```
---
### Project Structure
- __sql_and_dataframe__: example for processing data by 2 way: Spark SQL and Spark Dataframe 
- __schema_reader__: example for making schema and apply it into dataframe
---


Learning to better position !!!