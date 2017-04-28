package com.owner.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by admin on 2017/2/13.
 */
public class HelloWorldBolt extends BaseRichBolt {
    private static Logger LOGGER = LoggerFactory.getLogger(HelloWorldBolt.class);

    private int myCount = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    }

    @Override
    public void execute(Tuple tuple) {
        String test = tuple.getStringByField("sentence");
        if (test == "Hello World") {
            myCount++;
            LOGGER.info("Found a Hello World! My Count is now: " + Integer.toString(myCount));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
