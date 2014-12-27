package sn.analytics.factgen.processor;

import sn.analytics.factgen.type.AccessLogDatum;

/**
 * Created by Sumanth
 */
public interface DataProcessor {

    public void init();
    public void processDataItem(final AccessLogDatum accessLogDatum);

    public void close();
}
