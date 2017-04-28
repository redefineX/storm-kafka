package com.owner.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * Created by admin on 2017/2/13.
 */
public class HelloWorldSpout extends BaseRichSpout {
    private static Logger logger = LoggerFactory.getLogger(HelloWorldSpout.class);

    private SpoutOutputCollector collector;
    private int referenceRandom;
    private static final int MAX_RANDOM = 10;

    public HelloWorldSpout() {
        final Random rand = new Random();
        referenceRandom = rand.nextInt(MAX_RANDOM);
        logger.info("init random ... ...");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector collector) {
        init();
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        final Random rand = new Random();
        int instanceRandom = rand.nextInt(MAX_RANDOM);
        if (instanceRandom == referenceRandom) {
            collector.emit(new Values("Hello World"));
        } else {
            collector.emit(new Values("Other Random Word"));
        }
    }

    private void init(){
        logger.info("spout init called ... ...");
    }
}
