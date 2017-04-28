package com.owner.storm.kafka;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;
import com.dyuproject.protostuff.ProtobufIOUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.owner.storm.kafka.serializer.SerialType;
import com.owner.storm.kafka.serializer.Serializer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by admin on 2017/3/27.
 */
public class PartitionConsumer
{
	private static Logger LOG = LoggerFactory.getLogger(PartitionConsumer.class);
	private int partition;
	private KafkaConsumer kafkaConsumer;
	private Map conf;
	private KafkaSpoutConfig config;
	private ZkState zkState;
	private ConsumerFactory factory;
	private SerialType serialType = SerialType.nlmsg;
	private Serializer serializer;

	private LinkedList<MessageAndOffset> emittedMessages = new LinkedList<MessageAndOffset>();
	private SortedSet<Long> pendingOffsets = new TreeSet<Long>();
	private SortedSet<Long> failedOffsets = new TreeSet<Long>();
	private long emittedOffset;
	private long lastCommittedOffset;

	enum EmitState
	{
		EMIT_MORE,
		EMIT_END,
		EMIT_NONE
	}

	public PartitionConsumer(Map conf, KafkaSpoutConfig config, int partition, ZkState zkState)
	{
		this.conf = conf;
		this.config = config;
		this.partition = partition;
		this.zkState = zkState;
		this.kafkaConsumer = new KafkaConsumer(config);

		if (StringUtils.isNotEmpty(config.serialType))
		{
			SerialType _serialType = SerialType.getType(config.serialType);
			Preconditions.checkNotNull(_serialType, "config error, serial type [%s] is not supported ...", config.serialType);
			serialType = _serialType;
		}

		try
		{
			serializer = (Serializer) serialType.getClazz().newInstance();
		}
		catch (Exception e)
		{
			LOG.error("init kafka message serializer failed ...", e);
			throw new UnsupportedOperationException(String.format("init kafka message serializer failed, type: %s, class: %s ...", serialType.name(),
					serialType.getClazz().getName()), e);
		}

		Long jsonOffset = null;
		try
		{
			Map<Object, Object> json = zkState.readJSON(zkPath());
			if (json != null)
			{
				jsonOffset = (Long) json.get("offset");
			}
		}
		catch (Throwable e)
		{
			LOG.warn("Error reading and/or parsing at ZkNode: " + zkPath(), e);
		}

		try
		{
			if (config.fromBeginning)
			{
				emittedOffset = kafkaConsumer.getOffset(config.topic, partition, kafka.api.OffsetRequest.EarliestTime());
			}
			else if (config.startOffsetTime == -1)
			{
				emittedOffset = kafkaConsumer.getOffset(config.topic, partition, kafka.api.OffsetRequest.LatestTime());
			}
			else
			{
				if (jsonOffset == null)
				{
					lastCommittedOffset = kafkaConsumer.getOffset(config.topic, partition, kafka.api.OffsetRequest.LatestTime());
				}
				else
				{
					lastCommittedOffset = jsonOffset;
				}
				emittedOffset = lastCommittedOffset;
			}
		}
		catch (Exception e)
		{
			LOG.error("", e);
		}

		LOG.info("kafka consumer, partition: {}", partition);
	}

	public EmitState emit(SpoutOutputCollector collector)
	{
		int count = 0;
		try
		{
			if (emittedMessages.isEmpty())
			{
				boolean filled = fillMessage();
				if (!filled)
				{
					return EmitState.EMIT_END;
				}
			}

			while (true)
			{
				MessageAndOffset toEmitMsg = emittedMessages.pollFirst();
				if (toEmitMsg == null)
				{
					break;
				}

				collector.emit(toTuple(toEmitMsg.message()),
						new MessageMetaInfo(partition, toEmitMsg.offset()));
				//collector.emit(toTuple(toEmitMsg.message().payload()));
				//collector.emit(toTuple(toEmitMsg.message().payload()), toEmitMsg.offset());
				count++;
				if (count >= config.batchSendCount)
				{
					break;
				}
			}
		}
		catch (Exception e)
		{
			LOG.error(e.getMessage(), e);
		}
		if (emittedMessages.isEmpty() && count == 0)
		{
			return EmitState.EMIT_END;
		}
		else
		{
			return EmitState.EMIT_MORE;
		}
	}

	private boolean fillMessage()
	{
		ByteBufferMessageSet messageSet = null;
		try
		{
			long start = System.currentTimeMillis();
			long startOffset = emittedOffset;

			messageSet = kafkaConsumer.fetchMessages(partition, startOffset);

			if (null == messageSet)
			{
				return false;
			}

			int count = 0;
			for (MessageAndOffset message : messageSet)
			{
				count++;

				pendingOffsets.add(emittedOffset);
				emittedMessages.add(message);
				emittedOffset = message.nextOffset();
			}

			long end = System.currentTimeMillis();

			if (LOG.isDebugEnabled())
			{
				LOG.debug(
						"fetch message from partition:" + partition + ", start offset:" + startOffset + ", latest offset:" + emittedOffset + ", size:"
								+ messageSet.sizeInBytes() + ", count:" + count
								+ ", time:" + (end - start));
			}
		}
		catch (Exception e)
		{
			LOG.error("", e);
			return false;
		}

		return true;
	}

	public void commitState()
	{
		try
		{
			if (!failedOffsets.isEmpty())
			{
				long offset = failedOffsets.first();
				if (emittedOffset > offset)
				{
					emittedOffset = offset;
					pendingOffsets.tailSet(offset).clear();
				}
				failedOffsets.tailSet(offset).clear();
			}
			long lastOffset = lastCompletedOffset();
			if (lastOffset != lastCommittedOffset)
			{
				Map<Object, Object> data = new HashMap<Object, Object>();
				data.put("topology", conf.get("topology"));
				data.put("offset", lastOffset);
				data.put("partition", partition);
				data.put("broker",
						ImmutableMap.of("host", kafkaConsumer.getLeaderBroker().host(), "port", kafkaConsumer.getLeaderBroker().port()));
				data.put("topic", config.topic);
				zkState.writeJSON(zkPath(), data);
				lastCommittedOffset = lastOffset;
			}

		}
		catch (Exception e)
		{
			LOG.error(e.getMessage(), e);
		}
	}

	public long lastCompletedOffset()
	{
		long lastOffset = 0;
		if (pendingOffsets.isEmpty())
		{
			lastOffset = emittedOffset;
		}
		else
		{
			try
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("pendingOffsets size: {} ... ..." + pendingOffsets.size());
				}
				lastOffset = pendingOffsets.first();
			}
			catch (NoSuchElementException e)
			{
				lastOffset = emittedOffset;
			}
		}
		return lastOffset;
	}

	public void ack(long offset)
	{
		try
		{
			pendingOffsets.remove(offset);
		}
		catch (Exception e)
		{
			LOG.error("offset ack error " + offset);
		}
	}

	public void fail(long offset)
	{
		failedOffsets.add(offset);
	}

	public void close()
	{
		factory.removeConsumer(partition);
		kafkaConsumer.close();
	}

	private Values toTuple(Message message)
	{
		Values _value = new Values();

		ByteBuffer payload = message.payload();
		byte[] bytes = new byte[payload.limit()];
		payload.get(bytes);

		_value.add(serializer.unserialize(bytes));

		return _value;
	}

	private byte[] toByteArray(ByteBuffer buffer)
	{
		byte[] ret = new byte[buffer.remaining()];
		buffer.get(ret, 0, ret.length);
		return ret;
	}

	private String zkPath()
	{
		return config.zkRoot + "/kafka/offset/topic/" + config.topic + "/" + config.clientId + "/" + partition;
	}
}
