package sn.analytics.factgen.processor;



import com.google.common.base.Joiner;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import sn.analytics.factgen.type.AccessLogDatum;

import java.io.*;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

/**
 * Created by Sumanth
 */
public class FileDumper implements DataProcessor {
    private static final Logger logger = Logger.getLogger(FileDumper.class.getName());
    BufferedWriter writer =null;
    final String fileName ;
    boolean compress=false;


    public FileDumper(final String pathToFile, final boolean compress){
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
            String baseName ="/" +  FilenameUtils.getPathNoEndSeparator(fileName);
            
            
            BufferedWriter schemaFile = new BufferedWriter(new FileWriter(baseName+"/schema.csv"));

            schemaFile.write(Joiner.on(",").join(accessLogDatum.getFieldNames()));
            schemaFile.newLine();
            schemaFile.flush();
            schemaFile.close();
            
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
            logger.info("Output file:" + fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
