package com.unixera.storm.backport.utils;

/**
 * Created by Administrator on 2016/9/5.
 */

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


public class Utils extends backtype.storm.utils.Utils {
    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static Utils _instance = new Utils();

    /**
     * Provide an instance of this class for delegates to use.  To mock out
     * delegated methods, provide an instance of a subclass that overrides the
     * implementation of the delegated method.
     *
     * @param u a Utils instance
     * @return the previously set instance
     */
    public static Utils setInstance(Utils u) {
        Utils oldInstance = _instance;
        _instance = u;
        return oldInstance;
    }


    public static String localHostname () throws UnknownHostException {
        return _instance.localHostnameImpl();
    }

    // Non-static impl methods exist for mocking purposes.
    protected String localHostnameImpl () throws UnknownHostException {
        return InetAddress.getLocalHost().getCanonicalHostName();
    }
}
