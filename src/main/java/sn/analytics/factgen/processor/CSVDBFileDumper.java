package sn.analytics.factgen.processor;


import com.google.common.base.Joiner;
import sn.analytics.factgen.type.AccessLogDatum;

import java.io.*;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

/**
 * Created by Sumanth
 */
public class CSVDBFileDumper implements DataProcessor {
    private static final Logger logger = Logger.getLogger(CSVDBFileDumper.class.getName());
    BufferedWriter writer =null;
    final String fileName ;
    boolean compress=false;

    public CSVDBFileDumper(final String pathToFile){
        fileName = pathToFile;

    }

    public CSVDBFileDumper(final String pathToFile, final boolean compress){
        fileName = pathToFile;
        this.compress=compress;


    }
    @Override
    public void init() {
        try {
            if (!compress) {
                writer = new BufferedWriter(new FileWriter(fileName));
            }else{
                GZIPOutputStream zip = new GZIPOutputStream(new FileOutputStream(new File(fileName)));
                writer = new BufferedWriter(new OutputStreamWriter(zip, "UTF-8"));
            }
            //write the header
            AccessLogDatum accessLogDatum = new AccessLogDatum();
            writer.write(Joiner.on(",").join(accessLogDatum.getFieldNames()));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void processDataItem(final AccessLogDatum accessLogDatum) {
        try {


            writer.write(accessLogDatum.toCsv());
            writer.newLine();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {

        try {
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
