package sn.analytics.factgen.util.scheduledatagen;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import sn.analytics.factgen.FactGenerator;
import sn.analytics.factgen.processor.DataProcessor;
import sn.analytics.factgen.processor.FileDumper;
import sn.analytics.factgen.processor.ParquetDataProcessor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Created by Sumanth
 */
public class ScheduledDataGenerator {
    private static final Logger logger = Logger.getLogger(ScheduledDataGenerator.class.getName());

    private static ScheduledDataGenerator ourInstance = new ScheduledDataGenerator();
    private ScheduledExecutorService taskThread = Executors.newSingleThreadScheduledExecutor();

    static final DateTimeFormatter SECONDS_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    static
    final DateTimeFormatter dateToStrFormat = DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm-ss");

    public static ScheduledDataGenerator getInstance() {
        return ourInstance;
    }

    private ScheduledDataGenerator() {
    }
    
    enum DataProcessorType{
        CSV,
        PARQUET
    }
    private DataProcessorType dataProcessorType;
    private String outDir;
 
    private DateTime endTs;
    private DateTime curTs;
    
    private  int tpsForInterval = 60;
    private int runIntervalSeconds = 60;
    private int timeIntervalPerFile;
    private String hadoopConfDir;

    private class DataGenerator implements Runnable{

        public void run(){

            if (curTs.isAfter(endTs)){
                logger.info("Compplted data generation, shutting down");
                taskThread.shutdown();
                
            }
            
            String dataGenStartTs = curTs.toString(SECONDS_FORMAT);
            long startTsVal = curTs.getMillis();
            DateTime nextTs = curTs.plusSeconds(timeIntervalPerFile);
            String dataGenEndTs = nextTs.toString(SECONDS_FORMAT);

            curTs =nextTs;

            FactGenerator factGenerator = new FactGenerator();
            String fileExtension = ".csv";
            if (dataProcessorType== DataProcessorType.PARQUET){
                fileExtension=".parq";
            }
            final String outfile = StringUtils.replaceChars(outDir+"/"+"logdata-"+ startTsVal + "-" + curTs.getMillis() +fileExtension," ","-");
            DataProcessor dataProcessor = new ParquetDataProcessor(hadoopConfDir,outfile);
            dataProcessor.init();
            factGenerator.init(tpsForInterval);
            factGenerator.getProcessors().add(dataProcessor);
            factGenerator.generateFacts(dataGenStartTs, dataGenEndTs, tpsForInterval);
            factGenerator.finalizeResources();
            logger.info("Completed data generation for " + tpsForInterval +" from " + dataGenStartTs + " to " + dataGenEndTs);
            
        }
        
    }
    
    
 
    
    public void usage(){
        System.out.println("Usage: [File|Parquet] <StartTimestamp> <EndTimestamp> <TPS> <timeIntervalPerFile> <RunInterval> <outDir>  <hadoopConf>");
        System.exit(1);
        
    }
    
    public void init(final String fileFormat,
                     final String outDir,
                     String startTsStr,
                     String endTsStr,
                     final int tps,
                     final int timeIntervalPerFile,
                     final int runIntervalSeconds,
                     final String hadoopConf){

        if (fileFormat.equals("File")){
            dataProcessorType = DataProcessorType.CSV;
        }else if (fileFormat.equals("Parquet")){
            dataProcessorType = DataProcessorType.PARQUET;
        }else{
            usage();
            
        }
        
        curTs = DateTime.parse(startTsStr,SECONDS_FORMAT);
        endTs = DateTime.parse(endTsStr,SECONDS_FORMAT);
        logger.info("generate data from " + startTsStr + "  to " + endTsStr + " with tps " + tps);
        
        this.outDir = outDir;
        this.tpsForInterval=tps;
        this.timeIntervalPerFile  = timeIntervalPerFile;
        this.runIntervalSeconds=runIntervalSeconds;
        this.hadoopConfDir = hadoopConf;
       /* DataGenerator dataGenerator =  new DataGenerator();
        dataGenerator.run();*/
        taskThread.scheduleAtFixedRate(new DataGenerator(),0L, runIntervalSeconds, TimeUnit.SECONDS);


    }
      public static void main(String [] args){

        String fileFormat = args[0];
        String startTsStr = args[1];
        String endTsStr = args[2];
        int tps = Integer.valueOf(args[3]);
        int timeIntervalPerFile = Integer.valueOf(args[4]);
        
        int runAtIntervals = Integer.valueOf(args[5]);
        final String outDir = args[6];
        String hadoopConf=null;
        if (args.length > 6){
            hadoopConf = args[7];
        }
        
        ScheduledDataGenerator.getInstance().init(fileFormat,outDir,startTsStr,endTsStr,tps,timeIntervalPerFile,
                runAtIntervals,hadoopConf);
        
        //Parquet  "2014-11-13 11:00:00" "2014-11-13 11:05:00" 10 60 60 /logdatastore /opt/hadoop104/conf
    }
}
