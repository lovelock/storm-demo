package com.unixera.storm.util;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Frost Wong on 10/10/16.
 */
public class ServerConfigTest {
    @Test
    public void getZK() throws Exception {
        System.out.println(ServerConfig.getZK());
    }

    @Test
    public void getHdfs() throws Exception {
        System.out.println(ServerConfig.getHdfs());
    }

    @Test
    public void getHdfsRootDirectory() throws Exception {
        System.out.println(ServerConfig.getHdfsRootDirectory());
    }

    @Test
    public void getFieldSeparator() throws Exception {
        System.out.println(ServerConfig.getFieldSeparator());
    }

    @Test
    public void getPairSeparator() throws Exception {
        System.out.println(ServerConfig.getPairSeparator());
    }

    @Test
    public void getUrlSuffix() throws Exception {
        System.out.println(ServerConfig.getUrlSuffix());
    }

}