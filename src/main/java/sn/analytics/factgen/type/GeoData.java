package sn.analytics.factgen.type;


/**
 * Created by Sumanth
 */
public class GeoData {


    public final String region;
    public final String countryCode;
    public final String cityName;
    public final String continent;



    public GeoData(String region, String countryCode, String cityName, String continent) {
        this.region = region;
        this.countryCode = countryCode;
        this.cityName = cityName;
        this.continent = continent;
    }
}
