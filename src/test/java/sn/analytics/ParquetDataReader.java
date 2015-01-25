package sn.analytics;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.avro.AvroParquetReader;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import sn.analytics.type.LogData;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by Sumanth on 24/01/15.
 */
public class ParquetDataReader {
    
    public static void main(String [] args){
        //read from log store
        Configuration conf = new Configuration();

         final String hadoopConfDir="/opt/hadoop104/conf";
         final String inFilePath="/factstore/logdata1.parq";
         Schema FACT_SCHEMA;
        InputStream streamReader =ParquetDataReader.class.getResourceAsStream("/LogData.avsc");
        try {
            FACT_SCHEMA= Schema.parse(streamReader);
            conf.addResource(new Path(hadoopConfDir+"/core-site.xml"));
            conf.addResource(new Path(hadoopConfDir+"/hdfs-site.xml"));
            conf.addResource(new Path(hadoopConfDir+"/mapred-site.xml"));

            Path inFile= new Path(inFilePath);

      
        AvroParquetReader<LogData>
       
            logDataAvroParquetReader=
                    new AvroParquetReader<LogData>(conf,inFile);
            if (logDataAvroParquetReader == null) {System.out.println("IS NULL");}
            
            
            LogData logData = logDataAvroParquetReader.read();
            while(logData!=null){
                System.out.println(logData.getReceivedTimestamp() + " " + logData.getCity());
                logData = logDataAvroParquetReader.read();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        


    }
}
