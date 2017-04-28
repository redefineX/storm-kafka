package com.owner.storm.kafka2;

import com.owner.storm.kafka.KafkaSpoutConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by admin on 2017/4/27.
 */
public class TestConsumer
{
	public static void main(String[] args) throws Exception
	{
		Yaml yaml = new Yaml();
		Map<String, Object> yamlConfig = (Map<String, Object>) yaml
				.load(new InputStreamReader(new FileInputStream("F:\\intelliJidea\\stormdemo\\src\\main\\resources\\config.properties")));

		if (null == yamlConfig)
		{
			yamlConfig = new HashMap<String, Object>();
		}

		TestConsumer test = new TestConsumer();
		test.open(yamlConfig);

		while (true)
		{
			test.nextTuple();
		}
	}

	private long lastUpdateMs;
	private ConsumerFactory coordinator;
	private KafkaSpoutConfig config = new KafkaSpoutConfig();

	private final String fieldName = "kafka_message";

	public void nextTuple()
	{
		Collection<PartitionConsumer> partitionConsumers = coordinator.getPartitionConsumers();
		for (PartitionConsumer consumer : partitionConsumers)
		{
			consumer.emit(null);
		}

		long now = System.currentTimeMillis();
		if ((now - lastUpdateMs) > config.offsetUpdateIntervalMs)
		{
			commitState();
		}
	}

	public void open(Map stormConfig)
	{
		config.config(stormConfig);

		coordinator = new ConsumerFactory(stormConfig, config, null);
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
}
