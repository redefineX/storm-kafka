package com.owner.storm.kafka1;

import com.google.common.collect.Maps;
import com.owner.storm.kafka.Host;
import com.owner.storm.kafka.KafkaSpoutConfig;
import kafka.javaapi.message.ByteBufferMessageSet;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by admin on 2017/4/27.
 */
public class KafkaConsumer
{
	private KafkaSpoutConfig config;
	private String topic;
	private int partition;
	private long socketTimeoutMs;
	private org.apache.kafka.clients.consumer.KafkaConsumer consumer;
	private TopicPartition topicPartition;

	private Object lock = new Object();

	public KafkaConsumer(KafkaSpoutConfig config, String topic, int partition)
	{
		this.config = config;
		this.topic = topic;
		this.partition = partition;
		this.socketTimeoutMs = config.socketTimeoutMs;

		Properties props = new Properties();

		String serverPorts = "";
		List<Host> brokers = config.brokers;
		for (Host server : brokers)
		{
			serverPorts = serverPorts + server.getHost() + ":" + server.getPort() + ",";
		}

		props.put("bootstrap.servers", StringUtils.removeEnd(serverPorts, ","));
		props.put("group.id", config.clientId);
		props.put("client.id", config.clientId);
		props.put("enable.auto.commit", "false");
		props.put("session.timeout.ms", config.socketTimeoutMs);
		props.put("max.poll.records", config.fetchMaxBytes);
		props.put("fetch.max.bytes", config.fetchMaxBytes);
		props.put("max.partition.fetch.bytes", config.fetchMaxBytes);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(props);

		topicPartition = new TopicPartition(topic, partition);
		consumer.assign(Collections.singletonList(topicPartition));
	}

	public ConsumerRecords<String, byte[]> fetchMessages() throws IOException
	{
		ConsumerRecords<String, byte[]> records = consumer.poll(socketTimeoutMs);
		return records;
	}

	public void commitState(OffsetAndMetadata metadata)
	{
		consumer.commitSync(Collections.singletonMap(topicPartition, metadata));
	}

	public void seek(long offset)
	{
		consumer.seek(topicPartition, offset);
	}

	public void seekToBeginning()
	{
		consumer.seekToBeginning(Collections.singletonList(topicPartition));
	}

	public void close()
	{
		if (null != consumer)
		{
			consumer.close();
			consumer = null;
		}
	}
}
