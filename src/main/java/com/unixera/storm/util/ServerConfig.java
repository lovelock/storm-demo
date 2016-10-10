package com.unixera.storm.util;

/**
 * Created by Frost Wong on 10/10/16.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class ServerConfig {

    private static final Logger LOG = LoggerFactory.getLogger(ServerConfig.class);

    private static final String ZK_CONFIG_FILE = "zookeeper";
    private static final String HDFS_CONFIG_FILE = "hdfs";
    private static final String COMMON_CONFIG_FILE = "common";

    /**
     * 获取ZooKeeper的host地址
     *
     * @return String
     */
    public static String getZK() {
        StringBuilder builder = new StringBuilder();
        ResourceBundle bundle = ResourceBundle.getBundle(ZK_CONFIG_FILE, Locale.US);

        Enumeration<String> keys = bundle.getKeys();
        List<String> tmp = new ArrayList<>();

        for (; keys.hasMoreElements(); ) {
            String key = keys.nextElement();
            if (key.startsWith("zk.server")) {
                tmp.add(key);
            }
        }

        for (String aTmp : tmp) {
            builder.append(bundle.getString(aTmp)).append(",");
        }

        builder.deleteCharAt(builder.lastIndexOf(","));
        return builder.toString();
    }


    /**
     * 获取全局的HDFS集群配置
     *
     * @return String
     */
    public static String getHdfs() {
        StringBuilder builder = new StringBuilder();
        ResourceBundle bundle = ResourceBundle.getBundle(HDFS_CONFIG_FILE, Locale.US);
        builder.append(bundle.getString("hdfs.protocol"))
                .append("://")
                .append(bundle.getString("hdfs.host"))
                .append(":")
                .append(bundle.getString("hdfs.port"));

        return builder.toString();
    }

    public static String getHdfsRootDirectory() {
        String hdfsRootDirectory = ResourceBundle.getBundle(HDFS_CONFIG_FILE, Locale.US).getString("hdfs.rootDir");
        LOG.info("HDFS root directory is {}", hdfsRootDirectory);
        return hdfsRootDirectory;
    }


    public static String getFieldSeparator() {
        String fieldSeparator = ResourceBundle.getBundle(COMMON_CONFIG_FILE, Locale.US).getString("common.field_splitter");
        LOG.info("Field separator is {}", fieldSeparator);
        return fieldSeparator;
    }


    public static String getPairSeparator() {
        String pairSeparator = ResourceBundle.getBundle(COMMON_CONFIG_FILE, Locale.US).getString("common.pair_splitter");
        LOG.info("Pair separator is {}", pairSeparator);
        return pairSeparator;
    }

    public static String getUrlSuffix() {
        String urlSuffix = ResourceBundle.getBundle(COMMON_CONFIG_FILE, Locale.US).getString("common.url_suffix");
        LOG.info("Url suffix is {}", urlSuffix);
        return urlSuffix;
    }
}

