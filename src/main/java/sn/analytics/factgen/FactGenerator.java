package sn.analytics.factgen;

import com.google.common.base.Stopwatch;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import sn.analytics.factgen.processor.DBLoader;
import sn.analytics.factgen.processor.DataProcessor;
import sn.analytics.factgen.processor.FileDumper;
import sn.analytics.factgen.type.AccessLogDatum;
import sn.analytics.factgen.type.GeoData;
import sn.analytics.factgen.type.RequestUriData;
import sn.analytics.factgen.type.SessionData;
import sn.analytics.factgen.util.InputDataLoader;
import sn.analytics.factgen.util.IpAddressUtils;
import sn.analytics.factgen.util.UserAgentDataSet;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Created by Sumanth on 20/11/14.
 */
public class FactGenerator {

    private static Logger logger = Logger.getLogger(FactGenerator.class.getName());
    int maxClientIp = 50;
    int maxServerIp = 50;

    int maxUserAgents = 50;
    int maxClientIds = 1000;
     List<DataProcessor> processors = new ArrayList<DataProcessor>();

    Map<String, GeoData> geoDataMap = new HashMap<String, GeoData>();
    private List<RequestUriData> uridataBag = new ArrayList<RequestUriData>();

    private String[] clientIpSet = new String[maxClientIp];
    private String[] serverIpSet = new String[maxServerIp];

    private String [] clientIdSet = new String[maxClientIds];
    private SessionData curSessionData;
    Random rgen;

    //prob 0.75 success and 0.25 failure codes
    private ZipfDistribution responseCodeSample = new ZipfDistribution(Data.responseCodes.length, 2.5);

    GammaDistribution adder = new GammaDistribution(2, 10);

    long recordCount = 0;

    static final DateTimeFormatter MILL_SECONDS_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");

    static final DateTimeFormatter SECONDS_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    public FactGenerator() {
        rgen = new Random();
    }

    public FactGenerator(int seed) {
        rgen = new Random(seed);
    }

    public List<DataProcessor> getProcessors() {
        return processors;
    }

    public void setProcessors(List<DataProcessor> processors) {
        this.processors = processors;
    }


    private void generateClientIds(){
        Random random = new Random(10202);
        for(int i=0;i < maxClientIds;i++)
            clientIdSet[i] = "" + random.nextInt(20102);

    }

    private void initSessionData(){
        int userAgentIdx = rgen.nextInt(maxUserAgents);
        int clientHostIdx = rgen.nextInt(maxClientIp);
        curSessionData = new SessionData(UUID.randomUUID().toString(),
                clientIdSet[rgen.nextInt(clientIdSet.length)],
                userAgentIdx,clientHostIdx );

    }

    private AccessLogDatum buildDatum(final DateTime dateTimeIn,boolean flipSessionId) {

        if (flipSessionId)
            initSessionData();

        RequestUriData uridata = uridataBag.get(rgen.nextInt(uridataBag.size()));
        String target = Data.targetStrs[rgen.nextInt(Data.targetStrs.length)] + "/"+ uridata.getUriName();

        int statusCode = Data.responseCodes[responseCodeSample.sample() - 1];

        DateTime dateTime = dateTimeIn.toDateTime();


        AccessLogDatum accessLogDatum = new AccessLogDatum();

        accessLogDatum.requestVerb = uridata.verb;
        accessLogDatum.requestSize = uridata.getResponseSize() + (Math.abs(rgen.nextInt(50)));
        accessLogDatum.responseStatusCode = statusCode;

        accessLogDatum.receivedTimestamp = dateTimeIn.plusMillis(rgen.nextInt(500)).toString(MILL_SECONDS_FORMAT);

        accessLogDatum.clientId = curSessionData.getClientId();
        accessLogDatum.sessionId = curSessionData.getSessionId();
        accessLogDatum.accessUrl = target;

        int clientHostIdx = curSessionData.getClientIpIdx();
        accessLogDatum.clientIp = clientIpSet[clientHostIdx];

        accessLogDatum.dataExchangeSize = uridata.getResponseSize() + 2* Math.abs(rgen.nextInt(500)) ;

        int multiplerEffect = rgen.nextInt(6);
        int responseTimeAddenum = multiplerEffect * Double.valueOf(adder.sample()).intValue();

        //could math already generated data
        accessLogDatum.responseTime = uridata.responseTime + responseTimeAddenum + 2 * responseTimeAddenum;
        int userAgentIdx = curSessionData.getUserAgentIdx();
        String str = UserAgentDataSet.userAgentSet[userAgentIdx];

        String[] userAgentTkns = str.split(",");
        accessLogDatum.serverIp = serverIpSet[rgen.nextInt(maxServerIp)];

        accessLogDatum.clientId = clientIdSet[rgen.nextInt(maxClientIds)];
        // "Mobile Browser,7.1,iOS,7.0,Tablet,Mobile Safari",
        accessLogDatum.userAgentDevice = userAgentTkns[4];
        accessLogDatum.UserAgentType = userAgentTkns[0];
        accessLogDatum.userAgentFamily =userAgentTkns[5];
        accessLogDatum.userAgentOSFamily=userAgentTkns[2];
        accessLogDatum.userAgentOSVersion=userAgentTkns[1];
        accessLogDatum.userAgentVersion=userAgentTkns[3];

        GeoData geoData = geoDataMap.get(clientIpSet[clientHostIdx]);
        if (geoData != null) {
            accessLogDatum.city = geoData.cityName;
            accessLogDatum.country = geoData.countryCode;
            accessLogDatum.region = geoData.region;

        } else {

            //instead of unknown
            accessLogDatum.city = "Bangalore";
            accessLogDatum.country = "IN";
            accessLogDatum.region="India";

        }
        accessLogDatum.hourOfDay = dateTime.getHourOfDay();
        accessLogDatum.dayOfWeek = dateTime.getDayOfWeek();
        accessLogDatum.monthOfYear = dateTime.getMonthOfYear();
        accessLogDatum.minOfDay = dateTime.getMinuteOfDay();

        return accessLogDatum;

    }

    public void generateFacts(final String startTimeStr, final String endTimeStr, int tps) {

        DateTime curTs = DateTime.parse(startTimeStr, SECONDS_FORMAT);
        DateTime endTs = DateTime.parse(endTimeStr, SECONDS_FORMAT);
        logger.info("Generate access log data for " + startTimeStr + "[" + curTs.getMillis() +"]"
                + endTimeStr + "[" + endTs.getMillis() + "]" + " for " + tps );
        int maxTxnPerSessionPerUser = 3;
        if (tps > 500)
            maxTxnPerSessionPerUser=6;

        while (true) {

            int txnInSession =0;
            boolean flipSession = false;

            for (int i = 0; i < tps; i++) {
                txnInSession++;
                if (txnInSession > maxTxnPerSessionPerUser) {
                    flipSession = true;
                    txnInSession=0;

                }else{
                    flipSession=false;

                }

                AccessLogDatum accessLogDatum = buildDatum(curTs, flipSession);
                invokeProcessors(accessLogDatum);

            }
            curTs = curTs.plusSeconds(1);
            if (!curTs.isBefore(endTs)) {
                break;
            }
        }

    }

    private void invokeProcessors(final AccessLogDatum accessLogDatum) {
        for (DataProcessor dataProcessor : processors)
            dataProcessor.processDataItem(accessLogDatum);
    }

    public void finalizeResources() {
        for (DataProcessor dataProcessor : processors)
            dataProcessor.close();

    }

    public synchronized void init(int tps) {

       // orgs = InputDataLoader.readOrgNames();
        uridataBag = InputDataLoader.readURIs();
        rgen = new Random();
        //bring in the Ip set
        if (tps > 2000) {
            maxClientIp = 200;
            maxUserAgents = 200;
        }

        InputDataLoader.loadIpNames(maxClientIp, clientIpSet, geoDataMap);
        serverIpSet = IpAddressUtils.generateIpSet(maxServerIp);
        generateClientIds();
        initSessionData();

    }

    public static void usage() {
        System.out.println("Usage: <StartTimestamp> <EndTimestamp> <TPS> <Destination DB|File> <db.properties | FilePath>");
        System.exit(1);
    }

    public static void main(String[] args) {
        //if ()
        if (args.length < 4) usage();
        FactGenerator factGenerator = new FactGenerator();
        boolean waitForCompletion = false;
        //arguments are
        //start time, endtime


        for (int i = 3; i < args.length; i++) {

            if (args[i].startsWith("File")) {
                final String fileName = args[i].split("\\=")[1];
                DataProcessor fileProcessor = new FileDumper(fileName, false);
                fileProcessor.init();
                factGenerator.getProcessors().add(fileProcessor);
            } else if (args[i].startsWith("Parquet")) {


            }
        }
/*
        TODO: complete DBLoader to enable streaming directly to DB
        final String dbProps = args[i].split("\\=")[1];
        DataProcessor dbProcessor = new DBLoader(dbProps);
        dbProcessor.init();
        factGenerator.getProcessors().add(dbProcessor);
       */

        int tps= Integer.valueOf(args[2]);
        factGenerator.init(tps);
        Stopwatch sw = Stopwatch.createStarted();
        factGenerator.generateFacts(args[0], args[1],tps);
        sw.stop();
        if (waitForCompletion) {
            try {
                Thread.sleep(60 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        factGenerator.finalizeResources();

        logger.info("Completed data generation:" + sw.elapsed(TimeUnit.SECONDS) + " seconds");
    }

}
