package com.unixera.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.unixera.storm.backport.bolt.HdfsBolt;
import com.unixera.storm.backport.bolt.format.DefaultFileNameFormat;
import com.unixera.storm.backport.bolt.format.DelimitedRecordFormat;
import com.unixera.storm.backport.bolt.format.FileNameFormat;
import com.unixera.storm.backport.bolt.format.RecordFormat;
import com.unixera.storm.backport.bolt.rotation.FileRotationPolicy;
import com.unixera.storm.backport.bolt.rotation.FileSizeRotationPolicy;
import com.unixera.storm.backport.bolt.sync.CountSyncPolicy;
import com.unixera.storm.backport.bolt.sync.SyncPolicy;
import com.unixera.storm.backport.common.Partitioner;
import com.unixera.storm.util.Directory;
import com.unixera.storm.util.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;

/**
 * Created by Frost Wong on 10/10/16.
 */
public class LogStatisticsTopology {
    private static final Logger LOG = LoggerFactory.getLogger(LogStatisticsTopology.class);

    private static final String TOPIC = "storm-demo-topic";

    private static final String KAFKA_SPOUT_ID = "kafka-spout";
    private static final String CROP_BOLT_ID = "crop-bolt";
    private static final String SPLIT_FIELDS_BOLT_ID = "split-fields-bolt";
    private static final String STORM_HDFS_BOLT_ID = "storm-hdfs-bolt";

    private static final String TOPOLOGY_NAME = "storm-demo";

    public static void main(String[] args) {
        Config config = new Config();

        HdfsBolt hdfsBolt = makeHdfsBolt();
        KafkaSpout kafkaSpout = makeKafkaSpout(TOPIC, TOPOLOGY_NAME);

        LOG.info("Topology name is {}", TOPOLOGY_NAME);

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout(KAFKA_SPOUT_ID, kafkaSpout, 10);
        topologyBuilder.setBolt(CROP_BOLT_ID, new CropBolt(), 10).shuffleGrouping(KAFKA_SPOUT_ID);
        topologyBuilder.setBolt(SPLIT_FIELDS_BOLT_ID, new SplitFieldsBolt(), 10).shuffleGrouping(CROP_BOLT_ID);
        topologyBuilder.setBolt(STORM_HDFS_BOLT_ID, hdfsBolt, 10).fieldsGrouping(SPLIT_FIELDS_BOLT_ID, new Fields("timestamp", "fieldvalues"));

        if (args != null && args.length > 0) {
            config.setDebug(false);
            config.setNumWorkers(3);

            try {
                StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
            } catch (InvalidTopologyException | AlreadyAliveException | AuthorizationException e) {
                e.printStackTrace();
            }
        }
    }

    private static HdfsBolt makeHdfsBolt() {

        RecordFormat recordFormat = new DelimitedRecordFormat()
                .withFields(new Fields("fieldvalues"));

        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        FileRotationPolicy fileRotationPolicy = new FileSizeRotationPolicy(5.0F, FileSizeRotationPolicy.Units.MB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath(ServerConfig.getHdfsRootDirectory())
                .withExtension(".log");

        Partitioner partitioner = new Partitioner() {
            @Override
            public String getPartitionPath(Tuple tuple) {
                LOG.info("tuple field values: {}", tuple.getStringByField("fieldvalues"));
                return Directory.makeDefaultPath(tuple, TOPOLOGY_NAME);
            }
        };

        return new HdfsBolt()
                .withFsUrl(ServerConfig.getHdfs())
                .withRecordFormat(recordFormat)
                .withFileNameFormat(fileNameFormat)
                .withRotationPolicy(fileRotationPolicy)
                .withPartitioner(partitioner)
                .withSyncPolicy(syncPolicy);
    }

    private static KafkaSpout makeKafkaSpout(String topic, String client_id) {
        BrokerHosts brokerHosts = new ZkHosts(ServerConfig.getZK());

        SpoutConfig kafkaSpoutConfig = new SpoutConfig(brokerHosts, topic, "/" + topic, client_id);
        kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();

        return new KafkaSpout(kafkaSpoutConfig);
    }
}
