package sn.analytics.factgen.processor;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import sn.analytics.factgen.FactGenerator;
import sn.analytics.factgen.type.AccessLogDatum;
import sn.analytics.type.LogData;

import java.io.IOException;
import java.io.InputStream;

/**
 * Parquet, Avro based file processor
 * Created by Sumanth on 24/12/14.
 */
public class ParquetDataProcessor implements DataProcessor {

     private Logger logger = LoggerFactory.getLogger(ParquetDataProcessor.class);
    //to generate schema
    // java -jar avro-tools-1.7.7.jar compile schema /Users/Sumanth/codebase2/analytics/analytics/tools/factdatagenerator/src/main/resources/logData.avsc /tmp/src1/

    Configuration conf = new Configuration();

    private final String hadoopConfDir;
    private final String outFile;
    public    Schema FACT_SCHEMA;
    AvroParquetWriter<LogData> writer;
    Path outFilePath;

    public ParquetDataProcessor(String hadoopConfDir, String outFile) {
        this.hadoopConfDir = hadoopConfDir;
        this.outFile = outFile;
    }

    @Override
    public void init() {
        conf.addResource(new Path(hadoopConfDir+"/core-site.xml"));
        conf.addResource(new Path(hadoopConfDir+"/hdfs-site.xml"));
        conf.addResource(new Path(hadoopConfDir+"/mapred-site.xml"));
        System.out.println(conf.toString());
        initSchema();
        initWriter();

    }
    private void initSchema(){
        InputStream streamReader =ParquetDataProcessor.class.getResourceAsStream("/LogData.avsc");
        try {
            FACT_SCHEMA= Schema.parse(streamReader);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void initWriter(){
        outFilePath= new Path(outFile);

        try {
            writer=
                    new AvroParquetWriter<LogData>(outFilePath, FACT_SCHEMA,
                            CompressionCodecName.SNAPPY,
                            AvroParquetWriter.DEFAULT_BLOCK_SIZE,
                            AvroParquetWriter.DEFAULT_BLOCK_SIZE,
                            false,
                            conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void processDataItem(AccessLogDatum datum) {
        LogData logData = new LogData();
        logData.accessUrl=datum.accessUrl;
        logData.responseStatusCode=datum.responseStatusCode;
        logData.responseTime=datum.responseTime;
        logData.receivedTimestamp= DateTime.parse(datum.receivedTimestamp, FactGenerator.MILL_SECONDS_FORMAT).getMillis();

        logData.requestVerb=datum.requestVerb;
        logData.requestSize=datum.requestSize;
        logData.dataExchangeSize=datum.dataExchangeSize;
        logData.serverIp=datum.serverIp;
        logData.clientIp=datum.clientIp;
        logData.clientId=datum.clientId;
        logData.sessionId=datum.sessionId;
        logData.userAgentDevice=datum.userAgentDevice;
        logData.UserAgentType=datum.UserAgentType;
        logData.userAgentFamily=datum.userAgentFamily;
        logData.userAgentOSFamily=datum.userAgentOSFamily;
        logData.userAgentVersion=datum.userAgentVersion;
        logData.userAgentOSVersion=datum.userAgentOSVersion;
        logData.city=datum.city;
        logData.country=datum.country;
        logData.region=datum.region;
        logData.minOfDay=datum.minOfDay;
        logData.hourOfDay=datum.hourOfDay;
        logData.dayOfWeek=datum.dayOfWeek;
        logData.monthOfYear=datum.monthOfYear;


        try {
            writer.write(logData);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {

         logger.info("Writing completed to " + outFile);
        try{
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
