package sn.analytics.factgen.type;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Sumanth
 */
public class AccessLogDatum implements Serializable {

    //multi-tenant
    //public String appName;

    public String accessUrl;

    public int responseStatusCode;
    public int responseTime;

    public String receivedTimestamp;
    public String requestVerb;
    public int requestSize;
    public int dataExchangeSize;
    public String serverIp;
    //in yyyy-MM-dd HH:mm:ss.SSS
    public String clientIp;
    public String clientId;
    public String sessionId;
    //can be bulky
    //public String referrer;
    //User agent dimensions
    public String userAgentDevice;
    public String UserAgentType;
    public String userAgentFamily;
    public String userAgentOSFamily;
    public String userAgentVersion;
    public String userAgentOSVersion;

    //
    public String city;
    public String country;
    public String region;

    public int minOfDay;
    public int hourOfDay;
    public int dayOfWeek;
    public int monthOfYear;

    public String toCsv() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SIMPLE_STYLE);
    }

    public List<String> getFieldNames() {
        List<Field> fields =
                FieldUtils.getAllFieldsList(AccessLogDatum.class);
        List<String> fieldNames = new ArrayList<String>();
        for (Field fld : fields) {
            fieldNames.add(fld.getName());
        }

        return fieldNames;
    }



    public static void dumpAvroSchema(){

        List<Field> fields =
                FieldUtils.getAllFieldsList(AccessLogDatum.class);


        final String QUOTE ="\"";

        StringBuilder sb = new StringBuilder();
        for(Field fld : fields){
            //System.out.print(fld.getType().getName());
            sb.append("{").append(QUOTE).append("name").append(QUOTE).append(":").append("  ")
                    .append(QUOTE)
                    .append(fld.getName())
                    .append(QUOTE)
                    .append(" , ");
            sb.append(QUOTE).append("type").append(QUOTE).append(":");

            if (fld.getType().equals(String.class)){
                sb.append(QUOTE).append("string").append(QUOTE);
            }else if (fld.getType().getName().equals("int")){
                sb.append(QUOTE).append("int").append(QUOTE);
            }else{


            }
            sb.append("}").append(",");
            sb.append("\n");

            // break;
            //sb.append(fld.getName() +" " + )
        }

        System.out.println(sb.toString());

    }

    public static void printFieldNames(){
        List<Field> fields =
                FieldUtils.getAllFieldsList(AccessLogDatum.class);

        for(Field fld : fields){
            //System.out.print(fld.getType().getName());
            StringBuilder sb = new StringBuilder();
            sb.append("logData.");
            sb.append(fld.getName()).append("=").append("datum.").append(fld.getName());
            sb.append(";");
            //sb.append(fld.getName() +" " + )
            System.out.println(sb.toString());
        }

    }
    public static void main(String[] args) {

       // dumpAvroSchema();
        printFieldNames();

    }
}
