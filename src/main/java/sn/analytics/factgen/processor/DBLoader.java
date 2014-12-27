package sn.analytics.factgen.processor;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import sn.analytics.factgen.type.AccessLogDatum;

import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Created by Sumanth
 *
 */
//TODO: complete this
public class DBLoader implements DataProcessor {
    private static Logger logger = Logger.getLogger(DBLoader.class.getName());
    private Connection connection;
    private String connUrl;

    static final DateTimeFormatter MILL_SECONDS_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    PreparedStatement pstmt;


    String insertStmt ="";
    String host;
    int port;
    String dbName;
    String userName;
    String password;
    int batchRecordCount=0;
    int batchThreshold=1000;
    int batchCount=0;
    long recordCount=0;
    
    public DBLoader(final String dbProps){
        Properties props = new Properties();
        try {
            props.load(new FileReader(dbProps));
            host=props.getProperty("host","localhost");
            port =Integer.valueOf(props.getProperty("port","5432"));
            userName=props.getProperty("username","postgres");
            password=props.getProperty("password","postgres");
            dbName=props.getProperty("dbname","test");
            //connUrl = "jdbc:postgresql://localhost:5432/test";
            connUrl="jdbc:postgresql://" +host+":"+port+"/"+dbName;
            logger.info("Using DB connection "+connUrl);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void init() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            connection = DriverManager.getConnection(
                    connUrl, userName,password);
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            pstmt= connection.prepareStatement(insertStmt);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void processDataItem(final AccessLogDatum accessLogDatum) {

        addToBatch(accessLogDatum);
        if (batchRecordCount >=batchThreshold){

            commitData();
        }
        
        

    }
    private void commitData(){
        try {
            batchCount++;
            pstmt.executeBatch();
            connection.commit();
            logger.info("Succesfully committed batch:" + batchCount + " total records " + recordCount);
            batchRecordCount=0;

        } catch (SQLException e) {
            e.printStackTrace();
            e.getMessage();
        }
    }
    private void addToBatch(final AccessLogDatum accessLogDatum){
        try{
            //pstmt.setString(1,datum.appName);


        }catch (Exception e){
            e.printStackTrace();
        }
        try {
            //only for debugging
            // pstmt.execute();
            pstmt.addBatch();

            batchRecordCount++;
            recordCount++;
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            commitData();
            pstmt.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
