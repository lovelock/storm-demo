/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.unixera.storm.backport.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.unixera.storm.backport.bolt.format.FileNameFormat;
import com.unixera.storm.backport.bolt.format.RecordFormat;
import com.unixera.storm.backport.bolt.rotation.FileRotationPolicy;
import com.unixera.storm.backport.bolt.sync.SyncPolicy;
import com.unixera.storm.backport.common.AbstractHDFSWriter;
import com.unixera.storm.backport.common.HDFSWriter;
import com.unixera.storm.backport.common.Partitioner;
import com.unixera.storm.backport.common.rotation.RotationAction;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class HdfsBolt extends AbstractHdfsBolt {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsBolt.class);

    private transient FSDataOutputStream out;
    private RecordFormat format;

    public HdfsBolt withFsUrl(String fsUrl){
        this.fsUrl = fsUrl;
        return this;
    }

    public HdfsBolt withConfigKey(String configKey){
        this.configKey = configKey;
        return this;
    }

    public HdfsBolt withFileNameFormat(FileNameFormat fileNameFormat){
        this.fileNameFormat = fileNameFormat;
        return this;
    }

    public HdfsBolt withRecordFormat(RecordFormat format){
        this.format = format;
        return this;
    }

    public HdfsBolt withSyncPolicy(SyncPolicy syncPolicy){
        this.syncPolicy = syncPolicy;
        return this;
    }

    public HdfsBolt withRotationPolicy(FileRotationPolicy rotationPolicy){
        this.rotationPolicy = rotationPolicy;
        return this;
    }

    public HdfsBolt addRotationAction(RotationAction action){
        this.rotationActions.add(action);
        return this;
    }

    public HdfsBolt withTickTupleIntervalSeconds(int interval) {
        this.tickTupleInterval = interval;
        return this;
    }

    public HdfsBolt withRetryCount(int fileRetryCount) {
        this.fileRetryCount = fileRetryCount;
        return this;
    }

    public HdfsBolt withPartitioner(Partitioner partitioner) {
        this.partitioner = partitioner;
        return this;
    }

    public HdfsBolt withMaxOpenFiles(int maxOpenFiles) {
        this.maxOpenFiles = maxOpenFiles;
        return this;
    }

    @Override
    public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        LOG.info("Preparing HDFS Bolt...");
        this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
    }

    @Override
    protected String getWriterKey(Tuple tuple) {
        return "CONSTANT";
    }

    @Override
    protected AbstractHDFSWriter makeNewWriter(Path path, Tuple tuple) throws IOException {
        this.out = this.fs.create(path);
        return new HDFSWriter(rotationPolicy,path, out, format);
    }
}
