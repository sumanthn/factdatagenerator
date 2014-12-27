package sn.analytics.factgen;

/**
 * Created by Sumanth on 20/11/14.
 */
public class Data {

    public static int[] responseCodes = new int[]{
            200,//specifically set to 1 for zipf dist
            404,
            201,
            300,
            301,
            303,
            400,
            401,
            500
    };


   public static String [] targetStrs = new String[]{
            "EComm",
            "Search",
            "MapMaker",
            "DataCrunch"
    };

    public static String [] apiProduct = new String[]{
            "UI",
            "DB",
            "Monitor",
            "ML"

    };

}
