package com.owner.storm.kafka;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.channels.UnresolvedAddressException;
import java.util.*;

import kafka.cluster.BrokerEndPoint;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.KafkaException;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumer
{
	private static Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);
	public static final int NO_OFFSET = -1;

	private int status;
	private SimpleConsumer consumer = null;

	private KafkaSpoutConfig config;
	private LinkedList<Host> brokerList;
	private int brokerIndex;
	private BrokerEndPoint leaderBroker;
	private String clientId;
	private String topic;

	private int fetchMaxBytes;
	private int fetchWaitMaxMs;

	public KafkaConsumer(KafkaSpoutConfig config)
	{
		this.config = config;
		this.brokerList = new LinkedList<Host>(config.brokers);
		this.brokerIndex = 0;
		this.clientId = config.clientId + "_" + UUID.randomUUID();
		this.topic = config.topic;
		this.fetchMaxBytes = config.fetchMaxBytes;
		this.fetchWaitMaxMs = config.fetchWaitMaxMs;
	}

	public ByteBufferMessageSet fetchMessages(int partition, long offset) throws IOException
	{
		FetchRequest req = new FetchRequestBuilder().clientId(clientId).addFetch(topic, partition, offset, fetchMaxBytes)
				.maxWait(fetchWaitMaxMs).build();
		FetchResponse fetchResponse = null;
		SimpleConsumer simpleConsumer = null;
		try
		{
			simpleConsumer = findLeaderConsumer(partition);
			if (simpleConsumer == null)
			{
				return null;
			}
			fetchResponse = simpleConsumer.fetch(req);
		}
		catch (Exception e)
		{
			if (e instanceof ConnectException || e instanceof SocketTimeoutException || e instanceof IOException
					|| e instanceof UnresolvedAddressException)
			{
				LOG.warn("Network error when fetching messages:", e);
				if (simpleConsumer != null)
				{
					String host = simpleConsumer.host();
					int port = simpleConsumer.port();
					simpleConsumer = null;
					throw new KafkaException("Network error when fetching messages: " + host + ":" + port + " , " + e.getMessage(), e);
				}

			}
			else
			{
				throw new RuntimeException(e);
			}
		}
		if (fetchResponse.hasError())
		{
			short code = fetchResponse.errorCode(topic, partition);
			if (code == ErrorMapping.OffsetOutOfRangeCode() && config.resetOffsetIfOutOfRange)
			{
				long startOffset = getOffset(topic, partition, config.startOffsetTime);
				offset = startOffset;
			}
			else
			{
				if (leaderBroker != null)
				{
					LOG.info("WARNING: fetch data error from kafka topic[" + config.topic + "] host[" + leaderBroker.host() + ":" + leaderBroker
							.port()
							+ "] partition[" + partition + "] error:" + code);
				}

				if (code == ErrorMapping.NotLeaderForPartitionCode())
				{
					this.consumer = null;
				}

			}
			return null;
		}
		else
		{
			ByteBufferMessageSet msgs = fetchResponse.messageSet(topic, partition);
			return msgs;
		}
	}

	private SimpleConsumer findLeaderConsumer(int partition)
	{
		try
		{
			if (consumer != null)
			{
				return consumer;
			}
			PartitionMetadata metadata = findPartitionMeta(partition);
			if (metadata == null)
			{
				leaderBroker = null;
				consumer = null;
				return null;
			}
			leaderBroker = metadata.leader();
			consumer = new SimpleConsumer(leaderBroker.host(), leaderBroker.port(), config.socketTimeoutMs, config.socketReceiveBufferBytes,
					clientId);

			return consumer;
		}
		catch (Exception e)
		{
			LOG.error(e.getMessage(), e);
		}
		return null;
	}

	private PartitionMetadata findPartitionMeta(int partition)
	{
		PartitionMetadata returnMetaData = null;
		int errors = 0;
		int size = brokerList.size();
		int i = brokerIndex;
		loop:
		while (i < size && errors < size + 1)
		{
			Host host = brokerList.get(i);
			i = (i + 1) % size;
			brokerIndex = i; // next index
			try
			{
				LOG.info("findLeader broker: {},partition: {} ... ...", host.toString(), partition);
				consumer = new SimpleConsumer(host.getHost(), host.getPort(), config.socketTimeoutMs, config.socketReceiveBufferBytes,
						clientId);
				List<String> topics = Collections.singletonList(topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				TopicMetadataResponse resp = null;
				try
				{
					try
					{
						Thread.sleep(1000L);
					}
					catch (Exception e2)
					{

					}

					resp = consumer.send(req);
				}
				catch (Exception e)
				{
					errors += 1;

					LOG.error("findLeader error, broker:" + host.toString() + ",partition: " + partition + " will change to next broker index:"
							+ brokerIndex);
					if (consumer != null)
					{
						try
						{
							consumer.close();
							consumer = null;
						}
						catch (Exception e1)
						{
							LOG.warn("", e1);
						}
					}

					continue;
				}

				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData)
				{
					for (PartitionMetadata part : item.partitionsMetadata())
					{
						if (part.partitionId() == partition)
						{
							LOG.info("partition: {},meta: {}", partition, part.toString());
							returnMetaData = part;
							break loop;
						}
					}
				}

			}
			catch (Exception e)
			{
				LOG.error("Error communicating with Broker:" + host.toString() + ", find Leader for partition:" + partition);
			}
			finally
			{
				if (consumer != null)
				{
					consumer.close();
					consumer = null;
				}
			}
		}
		return returnMetaData;
	}

	public long getOffset(String topic, int partition, long startOffsetTime)
	{
		SimpleConsumer simpleConsumer = findLeaderConsumer(partition);

		if (simpleConsumer == null)
		{
			LOG.error("Error consumer is null get offset from partition:" + partition);
			return -1;
		}

		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(startOffsetTime, 1));
		OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), simpleConsumer.clientId());

		long[] offsets = simpleConsumer.getOffsetsBefore(request).offsets(topic, partition);
		if (offsets.length > 0)
		{
			return offsets[0];
		}
		else
		{
			return NO_OFFSET;
		}
	}

	public void close()
	{
		if (consumer != null)
		{
			consumer.close();
		}
	}

	public BrokerEndPoint getLeaderBroker()
	{
		return leaderBroker;
	}
}