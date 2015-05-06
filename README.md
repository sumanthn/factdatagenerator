#Fact Data generator
Synthetic data generator to generate access log data 
  
Characteristics of data generator<br>
The generated data set includes accessURL, responseTime, responseCode & sessionId
serverIp,clientIP,Client location [city,country,region -- geolocation fields]
Useragent details -- including agent family, agentOS.
Full details in class <em>sn.analytics.factgen.type.AccessLogDatum</em></br>
Response code follows <em>ZipfDistribution</em> with 3/4 success rate
Response time follows <em> LogNormalDistribution</em><br>
Tries to emulate client interaction within a session.<br>
Data can be generated for variable transactions per second and for a duration<br>
Data can be dumped into 
- CSV files[with schema in different file]
- Avro + Parquet format<br>
###Building
Build using maven<br>
<em>mvn clean package</em> <br>
generates target/factdatagenerator.jar [uber jar]

###Usage
<em>`[File|Avro|Parquet|ParquetAvro] <StartTimestamp> <EndTimestamp> <TPS> [filename] [HadoopConfDir]` </em>
Example generate data in CSV, <br>
`java -jar factdatagenerator.jar File "2015-01-25 11:06:00" "2014-01-25 11:07:00" 1000 /tmp/datadump/factnew.csv` <br>
Generates a CSV file with 1000 access records per second for a duration of 1 minute; the schema (header) is dumped separately into `schema.csv` in the same directory as factdata is generated<br>

To generate data in Avro format,<br>
`java -jar factdatagenerator.jar Parquet "2015-01-25 11:06:00" "2014-01-25 11:07:00" 1000 /logstore/logdata.avro /hadoop/conf`

To generate data in Parquet format,<br>
`java -jar factdatagenerator.jar Parquet "2015-01-25 11:06:00" "2014-01-25 11:07:00" 1000 /logstore/logdata.parq /hadoop/conf`

To generate data in Avro+Parquet format,<br>
`java -jar factdatagenerator.jar ParquetAvro "2015-01-25 11:06:00" "2014-01-25 11:07:00" 1000 /logstore/logdata.parq /hadoop/conf`

####Data dumps into multiple files
To simulate data dumps at regular intervals(ingestion of data into HDFS), ScheduledDataGenerator (`sn.analytics.factgen.util.scheduledatagen`) can be used<br>
Example:<br>
`Parquet  "2014-11-13 11:00:00" "2014-11-13 11:05:00" 10 60 60 /logdatastore /hadoop/conf` <br>
 Usage: `File|Parquet <StartTimestamp> <EndTimestamp> <TPS> <timeIntervalPerFile> <RunInterval> <outDir>  <hadoopConf>`
Generates data dump files of tps 60 every one minute into hdfs dir `logdatastore`
