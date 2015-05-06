package sn.analytics.factgen.processor;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by Sumanth on 06/05/15.
 */
public class AvroDataProcessor implements DataProcessor {
    private Logger logger = LoggerFactory.getLogger(ParquetDataProcessor.class);
    //to generate schema
    // java -jar avro-tools-1.7.7.jar compile schema <schemafile> <outdir>

    private final String outFile;
    public Schema FACT_SCHEMA;

    SpecificDatumWriter<LogData> writer;
    DataFileWriter<LogData> dataFileWriter;


    public AvroDataProcessor(String outFile) {
        this.outFile = outFile;
    }



    @Override
    public void init() {
       initSchema();
        initWriter();
    }

    @Override
    public void processDataItem(AccessLogDatum datum) {
        LogData logData = new LogData();
        logData.accessUrl=datum.accessUrl;
        logData.responseStatusCode=datum.responseStatusCode;
        logData.responseTime=datum.responseTime;
        logData.accessTimestamp= DateTime.parse(datum.accessTimestamp, FactGenerator.MILL_SECONDS_FORMAT).getMillis();

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
        logData.day=datum.day;


        try {
            dataFileWriter.append(logData);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {

        try {
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

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
       File outStream= new File(outFile);

        writer=new SpecificDatumWriter<LogData>(FACT_SCHEMA);
        dataFileWriter=new DataFileWriter<LogData>(writer);
        try {
            dataFileWriter.create(FACT_SCHEMA,outStream);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
