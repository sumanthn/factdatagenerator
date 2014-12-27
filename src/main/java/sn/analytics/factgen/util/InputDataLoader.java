package sn.analytics.factgen.util;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.reflect.FieldUtils;
import sn.analytics.factgen.type.AccessLogDatum;
import sn.analytics.factgen.type.GeoData;
import sn.analytics.factgen.type.RequestUriData;

import java.io.*;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Util methods to load various names and details from different files
 * Created by Sumanth
 */

public class InputDataLoader {
    private InputDataLoader(){

    }

    public static void loadIpNames(final int maxClientIp,final String [] clientIpSet,Map<String,GeoData> geoDataMap){
        Random rgen = new Random();
        InputStream inStream = ClassLoader.class.getResourceAsStream("/ipnames.csv");
        int count=0;
        //this will make client ip set totally not repeatable over iterations
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
            String line = reader.readLine();
            while(line!=null){

                String [] tkns = line.split(",");
                if (rgen.nextBoolean()) {
                    //geoResolvedIp.add(tkns[0]);
                    clientIpSet[count] = tkns[0];
                    GeoData geoData = new GeoData(tkns[1], tkns[2], tkns[3], tkns[4]);
                    geoDataMap.put(tkns[0], geoData);
                    count++;
                    if (count >= maxClientIp){
                        break;
                    }

                }

                line = reader.readLine();

            }

            reader.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public  static List<RequestUriData>  readURIs(){

        List<RequestUriData> uriDataBag = new ArrayList<RequestUriData>();
        BufferedReader reader = null;
        InputStream inStream = ClassLoader.class.getResourceAsStream("/uri.txt");

        try {
            reader = new BufferedReader(new InputStreamReader(inStream));

            String line = reader.readLine();
            while(line!=null){



                String [] tkns = line.split(",");
                RequestUriData requestUri = new RequestUriData(tkns[0],
                        tkns[3],
                        Integer.valueOf(tkns[2]),
                        Integer.valueOf(tkns[1]));
                uriDataBag.add(requestUri);

                line = reader.readLine();
            }

            return uriDataBag;

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (reader!=null)
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }

        return null;

    }






    public static void main(String [] args){
        List<Field> fields =
                FieldUtils.getAllFieldsList(AccessLogDatum.class);
        List<String> fieldNames = new ArrayList<String>();
        for(Field fld : fields){
            //System.out.print(fld.getName());
            fieldNames.add(fld.getName());
        }
        String header = Joiner.on(",").join(fieldNames);
        System.out.println(header);
    }
}
