package sn.analytics.factgen.type;

import com.google.common.base.Joiner;
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

    public static void generateSchema() {
        List<Field> fields =
                FieldUtils.getAllFieldsList(AccessLogDatum.class);

        System.out.println("CREATE TABLE groupbytest (");
        StringBuilder sb = new StringBuilder();
        for (Field fld : fields) {
            //System.out.print(fld.getType().getName());
            if (fld.getType().equals(String.class)) {
                if (fld.getName().contains("timestamp") || fld.getName().contains("time")) {
                    sb.append(fld.getName() + " " + "timestamp without time zone ");
                } else {
                    sb.append(fld.getName() + " " + "text");
                }
            } else if (fld.getType().getName().equals("int")) {
                sb.append(fld.getName() + " " + "int");
            } else {
                sb.append(fld.getName() + " " + "timestamp");

            }
            sb.append(",");
            sb.append("\n");

            //sb.append(fld.getName() +" " + )
        }

        System.out.println(sb.toString());
    }

    static void generateInsertInto() {
        List<Field> fields =
                FieldUtils.getAllFieldsList(AccessLogDatum.class);

        List<String> fldStrs = new ArrayList<String>();
        List<String> placeHolders = new ArrayList<String>();
        for (Field fld : fields) {
            fldStrs.add(fld.getName());
            placeHolders.add("?");
        }

        String insertIntoFlds = Joiner.on(",").join(fldStrs);
        String valPlaceHolders = Joiner.on(",").join(placeHolders);
        StringBuilder sb = new StringBuilder("INSERT INTO GROUPBYTEST ");
        sb.append("(").append(insertIntoFlds).append(")")
                .append(" VALUES ").append("(").append(valPlaceHolders).append(")");

        System.out.println(sb.toString());

        System.out.println();

        System.out.println("Num of fields:" + placeHolders.size());

    }

    static void generatePrepStmt() {
        List<Field> fields =
                FieldUtils.getAllFieldsList(AccessLogDatum.class);

        String prefix = "pstmt";

        int fldCount = 1;
        for (Field fld : fields) {
            StringBuilder sb = new StringBuilder();

            if (fld.getType().equals(String.class)) {

                if (fld.getName().contains("timestamp") || fld.getName().contains("time")) {

                    sb.append(prefix).append(".").append("setTimestamp(").append(fldCount)
                            .append(",new Timestamp(DateTime.parse(datum.").append(fld.getName())
                            .append(", MILL_SECONDS_FORMAT).getMillis()));");

                } else {
                    //setString(3, datum.apiproxy);
                    sb.append(prefix).append(".").append("setString(").
                            append(fldCount).append(",").append("datum").append(".").append(fld.getName()).append(")");

                }
            } else if (fld.getType().getName().equals("int")) {

                sb.append(prefix).append(".").append("setInt(").
                        append(fldCount).append(",").append("datum").append(".").append(fld.getName()).append(")");
            } else {
                System.out.println("unknown TYPE");

            }

            sb.append(";");
            System.out.println(sb.toString());

            fldCount++;

        }

    }

    public static void main(String[] args) {

        generateSchema();
        //generateInsertInto();
        //generatePrepStmt();

    }
}
