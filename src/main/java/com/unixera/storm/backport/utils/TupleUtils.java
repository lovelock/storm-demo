package com.unixera.storm.backport.utils;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2016/9/5.
 */
public class TupleUtils {
    private static final Logger LOG = LoggerFactory.getLogger(backtype.storm.utils.TupleUtils.class);

    public static boolean isTick(Tuple tuple) {
        return tuple != null
                && Constants.SYSTEM_COMPONENT_ID.equals(tuple.getSourceComponent())
                && Constants.SYSTEM_TICK_STREAM_ID.equals(tuple.getSourceStreamId());
    }

    public static <T> int listHashCode(List<T> alist) {
        if (alist == null) {
            return 1;
        } else {
            return Arrays.deepHashCode(alist.toArray());
        }
    }

    public static Map<String, Object> putTickFrequencyIntoComponentConfig(Map<String, Object> conf, int tickFreqSecs) {
        if (conf == null) {
            conf = new Config();
        }

        if (tickFreqSecs > 0) {
            LOG.info("Enabling tick tuple with interval [{}]", tickFreqSecs);
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFreqSecs);
        }

        return conf;
    }

}
