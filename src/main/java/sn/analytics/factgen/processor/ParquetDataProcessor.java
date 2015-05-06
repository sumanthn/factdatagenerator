package sn.analytics.factgen.processor;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.io.*;
import org.joda.time.DateTime;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import sn.analytics.factgen.FactGenerator;
import sn.analytics.factgen.type.AccessLogDatum;
import sn.analytics.type.LogData;

import java.io.*;

/**
 * Created by Sumanth on 05/05/15.
 */
public class ParquetDataProcessor implements DataProcessor{


    Configuration conf = new Configuration();

    private final String hadoopConfDir;
    private final String outFile;
    MessageType FACT_SCHEMA;
    ParquetWriter writer;
    Path outFilePath;

    public ParquetDataProcessor(String hadoopConfDir, String outFile){
        this.hadoopConfDir = hadoopConfDir;
        this.outFile = outFile;
    }


    @Override
    public void init() {
        conf.addResource(new Path(hadoopConfDir+"/core-site.xml"));
        conf.addResource(new Path(hadoopConfDir+"/hdfs-site.xml"));
        conf.addResource(new Path(hadoopConfDir+"/mapred-site.xml"));

        initSchema();
        initWriter();
    }

    @Override
    public void processDataItem(AccessLogDatum datum) {


        //Writable[] rec = new Writable[28];
        Writable[] rec = new Writable[25];


        rec[0]= new BytesWritable(datum.accessUrl.getBytes());
        rec[1]= new IntWritable(datum.responseStatusCode);
        rec[2]= new IntWritable(datum.responseTime);
        rec[3]= new BytesWritable(datum.accessTimestamp.getBytes());
        rec[4]= new BytesWritable(datum.requestVerb.getBytes());
        rec[5]= new IntWritable(datum.requestSize);
        rec[6]= new IntWritable(datum.dataExchangeSize);
        rec[7]= new BytesWritable(datum.serverIp.getBytes());
        rec[8]= new BytesWritable(datum.clientIp.getBytes());
        rec[9]= new BytesWritable(datum.clientId.getBytes());
        rec[10]= new BytesWritable(datum.sessionId.getBytes());
        rec[11]= new BytesWritable(datum.userAgentDevice.getBytes());
        rec[12]= new BytesWritable(datum.UserAgentType.getBytes());
        rec[13]= new BytesWritable(datum.userAgentFamily.getBytes());
        rec[14]= new BytesWritable(datum.userAgentOSFamily.getBytes());
        rec[15]= new BytesWritable(datum.userAgentVersion.getBytes());
        rec[16]= new BytesWritable(datum.userAgentOSVersion.getBytes());
        rec[17]= new BytesWritable(datum.city.getBytes());
        rec[18]= new BytesWritable(datum.country.getBytes());
        rec[19]= new BytesWritable(datum.region.getBytes());
        rec[20]= new IntWritable(datum.minOfDay);
        rec[21]= new IntWritable(datum.hourOfDay);
        rec[22]= new IntWritable(datum.dayOfWeek);
        rec[23]= new IntWritable(datum.monthOfYear);
        rec[24]= new BytesWritable(datum.day.getBytes());
/*

        rec[25]= new BytesWritable("Var1".getBytes());
        rec[26]= new BytesWritable("Var2".getBytes());
        rec[27]= new BytesWritable("Var3".getBytes());
*/


        ArrayWritable recordData = new ArrayWritable(Writable.class, rec);


        try {
            writer.write(recordData);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {

        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initSchema(){
        InputStream streamReader =ParquetDataProcessor.class.getResourceAsStream("/LogData.parq");
        StringBuilder schema = new StringBuilder();

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(streamReader, "UTF-8"));
            String line = br.readLine();
            while(line!=null){
                schema.append(line.trim());
                schema.append(" ");

                line = br.readLine();
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        FACT_SCHEMA= MessageTypeParser.parseMessageType(schema.toString());
       // System.out.println(FACT_SCHEMA);

    }
    private void initWriter(){
        outFilePath= new Path(outFile);
        DataWritableWriteSupport.setSchema(FACT_SCHEMA, conf);
        try {
            writer=new ParquetWriter(outFilePath, new DataWritableWriteSupport () {
                @Override
                public WriteContext init(Configuration configuration) {
                    if (configuration.get(DataWritableWriteSupport.PARQUET_HIVE_SCHEMA) == null) {
                        configuration.set(DataWritableWriteSupport.PARQUET_HIVE_SCHEMA, FACT_SCHEMA.toString());
                    }
                    return super.init(configuration);
                }
            },CompressionCodecName.SNAPPY, 256*1024*1024, 100*1024);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
