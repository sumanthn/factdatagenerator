package sn.analytics.factgen.util.scheduledatagen;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import sn.analytics.factgen.FactGenerator;
import sn.analytics.factgen.processor.DataProcessor;
import sn.analytics.factgen.processor.FileDumper;

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

    private String outDir;
    private int tps;
    private  int tpsForInterval = 60;
    private int runIntervalSeconds = 60;

    private  class DataGenerator implements Runnable{


        @Override
        public void run() {
            final DateTime startTs = DateTime.now().minusSeconds(tpsForInterval); //.secondOfMinute().roundFloorCopy();
            final String fileNameSuffix = "datadump-"+startTs.toString(dateToStrFormat)+".gz";
            final String fileName = outDir +"/" + fileNameSuffix;
            logger.info("Started data generation " + startTs.toString(SECONDS_FORMAT) + " for " + tps);
            FactGenerator factGenerator = new FactGenerator();
            DataProcessor fileProcessor = new FileDumper(fileName,true);
            fileProcessor.init();
            factGenerator.getProcessors().add(fileProcessor);
            factGenerator.init(tps);

            DateTime endTs = startTs.plusSeconds(tpsForInterval);
            factGenerator.generateFacts(startTs.toString(SECONDS_FORMAT),endTs.toString(SECONDS_FORMAT),tps);
            factGenerator.finalizeResources();


             logger.info("Completed data generation " + startTs.toString(SECONDS_FORMAT)
                    + " to " + endTs.toString(SECONDS_FORMAT)  + " tps " + tps);

        }
    }
    public void init(final String outDir,final int tps,final int intervalSeconds,
                     final int runIntervalSeconds,final String metaDataDbProps){

        this.outDir = outDir;
        this.tps = tps;
        this.tpsForInterval=intervalSeconds;
        this.runIntervalSeconds=runIntervalSeconds;
         taskThread.scheduleAtFixedRate(new DataGenerator(),0L, runIntervalSeconds, TimeUnit.SECONDS);


    }

    //CMD ARGS
    // /tmp/datadump 100 60 120 /Users/Sumanth/aggregation/filerepo.properties
    public static void main(String [] args){


        ScheduledDataGenerator.getInstance().init(args[0],
                Integer.valueOf(args[1]),
                Integer.valueOf(args[2]),
                Integer.valueOf(args[3]),
                args[4]);

        //ScheduledDataGenerator.getInstance().kickStart();

    }
}
