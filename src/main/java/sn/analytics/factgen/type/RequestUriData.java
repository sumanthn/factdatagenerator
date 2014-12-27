package sn.analytics.factgen.type;


/**
 * Created by Sumanth
 */
public class RequestUriData {
    public final String uriName;
    public final String verb;
    public final int responseSize;
    public final int responseTime;

    public RequestUriData(String uri,
                          String verb, int responseSize, int responseTime) {
        this.uriName = uri;
        this.verb = verb;
        this.responseSize = responseSize;
        this.responseTime = responseTime;
    }

    public String getUriName() {
        return uriName;
    }

    public String getVerb() {
        return verb;
    }

    public int getResponseSize() {
        return responseSize;
    }

    public int getResponseTime() {
        return responseTime;
    }
}
