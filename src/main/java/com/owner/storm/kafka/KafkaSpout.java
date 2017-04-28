package com.owner.storm.kafka;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * Created by admin on 2017/3/20.
 * https://github.com/feilaoda/jstorm-kafka/blob/master/src/main/java/com/alibaba/jstorm/kafka/KafkaSpout.java
 */
public class KafkaSpout implements IRichSpout
{
	private static Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

	protected SpoutOutputCollector collector;

	private long lastUpdateMs;
	private ConsumerFactory coordinator;
	private KafkaSpoutConfig config;
	private ZkState zkState;

	private final String fieldName = "kafka_message";

	public KafkaSpout()
	{
		config = new KafkaSpoutConfig();
	}

	@Override
	public void nextTuple()
	{
		Collection<PartitionConsumer> partitionConsumers = coordinator.getPartitionConsumers();
		for (PartitionConsumer consumer : partitionConsumers)
		{
			consumer.emit(collector);
		}

		long now = System.currentTimeMillis();
		if ((now - lastUpdateMs) > config.offsetUpdateIntervalMs)
		{
			commitState();
		}
	}

	@Override
	public void open(Map stormConfig, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector)
	{
		this.collector = spoutOutputCollector;

		config.config(stormConfig);
		zkState = new ZkState(stormConfig, config);

		coordinator = new ConsumerFactory(stormConfig, config, topologyContext, zkState);
		coordinator.createConsumerPartition();

		lastUpdateMs = System.currentTimeMillis();

	}

	public void commitState()
	{
		lastUpdateMs = System.currentTimeMillis();
		for (PartitionConsumer consumer : coordinator.getPartitionConsumers())
		{
			consumer.commitState();
		}

	}

	@Override
	public void close()
	{
		zkState.close();
	}

	@Override
	public void activate()
	{

	}

	@Override
	public void deactivate()
	{

	}

	@Override
	public void ack(Object o)
	{
		if (o instanceof MessageMetaInfo)
		{
			MessageMetaInfo metaInfo = (MessageMetaInfo) o;
			coordinator.getConsumer(metaInfo.getPartition()).ack(metaInfo.getOffset());
		}
	}

	@Override
	public void fail(Object o)
	{
		if (o instanceof MessageMetaInfo)
		{
			MessageMetaInfo metaInfo = (MessageMetaInfo) o;
			coordinator.getConsumer(metaInfo.getPartition()).fail(metaInfo.getOffset());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
	{
		outputFieldsDeclarer.declare(new Fields(fieldName));
	}

	@Override
	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}
}
