package sn.analytics.factgen.type;

/**
 * Created by Sumanth
 */
public class SessionData {

    private String sessionId;
    private String clientId;
    //just maintain the indexes
    private int userAgentIdx;
    private int clientIpIdx;

    public SessionData(String sessionId, String clientId, int userAgentIdx, int clientIpIdx) {
        this.sessionId = sessionId;
        this.clientId = clientId;
        this.userAgentIdx = userAgentIdx;
        this.clientIpIdx = clientIpIdx;
    }

    public String getSessionId() {
        return sessionId;
    }

    public String getClientId() {
        return clientId;
    }

    public int getUserAgentIdx() {
        return userAgentIdx;
    }

    public int getClientIpIdx() {
        return clientIpIdx;
    }
}
