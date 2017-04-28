package com.owner.storm.tool;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.owner.storm.kafka.serializer.NLMessage;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by admin on 2017/4/26.
 */
public class CountBolt extends BaseRichBolt
{
	private static Logger LOGGER = LoggerFactory.getLogger(CountBolt.class);
	private OutputCollector outputCollector;
	private int myCount = 0;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector)
	{
		this.outputCollector = outputCollector;
	}

	@Override
	public void execute(Tuple tuple)
	{
		//String test = tuple.getStringByField("kafka_message");
		myCount++;
		if (myCount % 10000 == 1)
		{
			LOGGER.info("Found a Hello World! My Count is now: " + Integer.toString(myCount));
			NLMessage msg = (NLMessage) tuple.getValue(0);
			LOGGER.info("------0-----{}", tuple.size());
			LOGGER.info("------1-----{}", ToStringBuilder.reflectionToString(msg));
			LOGGER.info("------2-----{}", new String(msg.getBody()));
		}
		//outputCollector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
	{
		outputFieldsDeclarer.declare(new Fields("hehe"));
	}
}
