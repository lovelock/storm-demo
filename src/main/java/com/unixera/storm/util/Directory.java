package com.unixera.storm.util;

import backtype.storm.tuple.Tuple;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Frost Wong on 10/10/16.
 */
public class Directory {
    private static final Logger LOG = LoggerFactory.getLogger(Directory.class);

    public static String makeDefaultPath(Tuple tuple, String topologyName) {
        String timestamp = tuple.getStringByField("timestamp");

        LOG.info("timestamp => {}", timestamp);

        DateTime dateTime = new DateTime(timestamp);
        String dayPath = dateTime.getDayWithYearAndMonth();

        LOG.info("dayPath => {}", dayPath);

        String hour = dateTime.getHour();

        LOG.info("hour => {}", hour);
        String path = Path.SEPARATOR + topologyName + Path.SEPARATOR + dayPath + Path.SEPARATOR + hour;

        LOG.info("{}'s HDFS write directory is {}", topologyName, path);

        return path;
    }
}