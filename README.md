## Example for SparkSQL vs Spark DataFrame

Link to get Data: [Data](https://data.sfgov.org/Public-Safety/Fire-Department-Calls-For-Service-2016-/kikm-y2iv/about_data)

Notice: Add this line to spark-defaults.conf: 
```
spark.driver.extraJavaOptions -Dlog4j.configuration=file:../log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=hello-spark
```

Learning to better position !!!