package com.unixera.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.unixera.storm.util.ServerConfig;
import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 将从KafkaSpout拿到的数据根据需求切分成不同的field,以便发送给后续的Bolt
 * Created by Frost Wong on 10/10/16.
 */
public class CropBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(CropBolt.class);
    /**
     * 数据容器
     */
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        // todo 写注释
        String message = tuple.getString(0);

        // to avoid NullPointerException
        if (message != null) {
            String domain, service, timestamp;

            HashMap map = makeMapOfMessage(message);
            domain = (String) map.get("domain");
            LOG.info("domain name of message {} is {}", tuple.getMessageId(), domain);
            timestamp = (String) map.get("time_local");
            LOG.info("timestamp of message {} is {}", tuple.getMessageId(), timestamp);

            if (domain.endsWith(ServerConfig.getUrlSuffix())) {
                service = domain.split("\\.")[0];
                collector.emit(tuple, new Values(timestamp, message, service));
                collector.ack(tuple);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp", "message", "service"));
    }

    /**
     * 将以特定字符分隔的字符串改造成键值对
     *
     * @param message 以fieldSeparator为字段分隔符,以pairSeparator为键值分隔符的字符串
     * @return HashMap 返回键值对组
     */
    private HashMap makeMapOfMessage(String message) {
        String[] fields = message.split(ServerConfig.getFieldSeparator());
        HashMap<String, String> map = new HashMap<>();

        try {
            for (String field : fields) {
                String[] pair = field.split(ServerConfig.getPairSeparator(), 2);
                map.put(pair[0], pair[1]);
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            e.printStackTrace();
        }

        return map;
    }
}
