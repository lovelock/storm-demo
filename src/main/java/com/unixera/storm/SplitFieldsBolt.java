package com.unixera.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.unixera.storm.util.ServerConfig;

import java.util.Map;

/**
 * Created by Frost Wong on 10/10/16.
 */
public class SplitFieldsBolt extends BaseRichBolt {

    private static final String SPLITTER = "\t";

    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        // 其中的field就是刚才outputDeclarer中定义的
        String timestamp = tuple.getStringByField("timestamp");
        String message = tuple.getStringByField("message");
        String service = tuple.getStringByField("service");

        try {
            String[] fields = message.split(ServerConfig.getFieldSeparator());
            StringBuilder stringBuilder = new StringBuilder();

            for (String field : fields) {

                if (field != null) {
                    String[] pair = field.split(ServerConfig.getPairSeparator(), 2);
                    stringBuilder.append(pair[1]).append(SPLITTER);
                }
            }
            stringBuilder.append(service);

            collector.emit(tuple, new Values(timestamp, stringBuilder.toString()));
            collector.ack(tuple);
        } catch (ArrayIndexOutOfBoundsException e) {
            collector.fail(tuple);
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp", "fieldvalues"));
    }
}
