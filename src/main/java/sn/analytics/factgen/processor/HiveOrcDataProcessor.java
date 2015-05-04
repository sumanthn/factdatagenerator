package sn.analytics.factgen.processor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.OutputFormat;
import sn.analytics.factgen.type.AccessLogDatum;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Sumanth on 26/04/15.
 */
public class HiveOrcDataProcessor implements DataProcessor {

    private final OrcSerde serde = new OrcSerde();

    //Define the struct which will represent each row in the ORC file
    private final String typeString = "struct<accessUrl:string,responseStatusCode:int,responseTime:int,accessTimestamp:string,requestVerb:string,requestSize:int,dataExchangeSize:int,serverIp:string,clientIp:string,clientId:string,sessionId:string,userAgentDevice:string,UserAgentType:string,userAgentFamily:string,userAgentOSFamily:string,userAgentVersion:string,userAgentOSVersion:string,city:string,country:string,region:string,minOfDay:int,hourOfDay:int,dayOfWeek:int,monthOfYear:int,day:string>";


    private final TypeInfo typeInfo = TypeInfoUtils
            .getTypeInfoFromTypeString(typeString);
    private final ObjectInspector oip = TypeInfoUtils
            .getStandardJavaObjectInspectorFromTypeInfo(typeInfo);



    Configuration conf = new Configuration();
    FSDataOutputStream out;

    private RecordWriter  writer;
    private final String hadoopConfDir;
    private final String outFile;

    public HiveOrcDataProcessor(String hadoopConfDir, String outFile) {
        this.hadoopConfDir = hadoopConfDir;
        this.outFile = outFile;
    }


    @Override
    public void init() {
        JobConf jConf = new JobConf();
        conf.addResource(new Path(hadoopConfDir+"/core-site.xml"));
        conf.addResource(new Path(hadoopConfDir+"/hdfs-site.xml"));
        conf.addResource(new Path(hadoopConfDir+"/mapred-site.xml"));
        try {
            FileSystem fs = FileSystem.get(conf);
            Path outFilePath = new Path(outFile);
            fs.create(outFilePath);

            OrcOutputFormat outFormat = new OrcOutputFormat();

             writer = outFormat.getRecordWriter(fs, jConf,
                   outFile, Reporter.NULL);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(conf.toString());
    }

    @Override
    public void processDataItem(AccessLogDatum accessLogDatum) {

        List<Object> orcContainer =new ArrayList<Object>(20);
        orcContainer.add(0,accessLogDatum.accessUrl);
        orcContainer.add(1,accessLogDatum.responseStatusCode);
        orcContainer.add(2,accessLogDatum.responseTime);
        orcContainer.add(3,accessLogDatum.accessTimestamp);
        orcContainer.add(4,accessLogDatum.requestVerb);
        orcContainer.add(5,accessLogDatum.requestSize);
        orcContainer.add(6,accessLogDatum.dataExchangeSize);
        orcContainer.add(7,accessLogDatum.serverIp);
        orcContainer.add(8,accessLogDatum.clientIp);
        orcContainer.add(9,accessLogDatum.clientId);
        orcContainer.add(10,accessLogDatum.sessionId);
        orcContainer.add(11,accessLogDatum.userAgentDevice);
        orcContainer.add(12,accessLogDatum.UserAgentType);
        orcContainer.add(13,accessLogDatum.userAgentFamily);
        orcContainer.add(14,accessLogDatum.userAgentOSFamily);
        orcContainer.add(15,accessLogDatum.userAgentVersion);
        orcContainer.add(16,accessLogDatum.userAgentOSVersion);
        orcContainer.add(17,accessLogDatum.city);
        orcContainer.add(18,accessLogDatum.country);
        orcContainer.add(19,accessLogDatum.region);
        orcContainer.add(20,accessLogDatum.minOfDay);
        orcContainer.add(21,accessLogDatum.hourOfDay);
        orcContainer.add(22,accessLogDatum.dayOfWeek);
        orcContainer.add(23,accessLogDatum.monthOfYear);
        orcContainer.add(24,accessLogDatum.day);

        try {
            writer.write(NullWritable.get(),serde.serialize(orcContainer,oip));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() {

        try {
            writer.close(Reporter.NULL);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
