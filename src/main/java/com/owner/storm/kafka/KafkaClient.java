package com.owner.storm.kafka;

import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

/**
 * Created by admin on 2017/3/21.
 */
public class KafkaClient
{
	public static final String DEFAULT_KEY_DESERIALIZER =
			"org.apache.kafka.common.serialization.StringDeserializer";
	public static final String DEFAULT_VALUE_DESERIAIZER =
			"org.apache.kafka.common.serialization.ByteArrayDeserializer";
	public static final String DEFAULT_AUTO_OFFSET_RESET = "earliest";

	private org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> consumer;

	public static void main ( String[] args )
	{
		KafkaClient client = new KafkaClient ();

		Properties consumerProps = new Properties ();
		consumerProps.put (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DEFAULT_KEY_DESERIALIZER);
		consumerProps.put (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_DESERIAIZER);
		consumerProps.put (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, DEFAULT_AUTO_OFFSET_RESET);
		//These always take precedence over config
		consumerProps.put (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.32.148.67:19092,172.32.148.68:19092,172.32.148.69:19092,172.32.148.70:19092,172.32.148.71:19092");
		consumerProps.put (ConsumerConfig.GROUP_ID_CONFIG, "intelliJ");
		consumerProps.put (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> con = new org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> (
				consumerProps);
		con.subscribe (Arrays.asList ("TOPIC_CONSUMER"), new ConsumerRebalanceListener ()
		{
			@Override
			public void onPartitionsRevoked ( Collection<TopicPartition> collection )
			{
				System.out.println ("revoke");
			}

			@Override
			public void onPartitionsAssigned ( Collection<TopicPartition> collection )
			{
				System.out.println ("assign");
			}
		});

		client.setConsumer (con);

		ConsumerRecords<String, byte[]> records = con.poll (1000L);
		Iterator<ConsumerRecord<String, byte[]>> recordIte = records.iterator ();
		while (true)
		{

			try
			{
				while (recordIte.hasNext ())
				{
					System.out.println (recordIte.next ().value ());
				}

				Thread.sleep (1000L);

				records = con.poll (1000L);
				recordIte = records.iterator ();
			}
			catch (Exception e)
			{

			}
		}

	}

	public void setConsumer ( org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> consumer )
	{
		this.consumer = consumer;
	}
}
