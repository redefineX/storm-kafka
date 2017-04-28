package com.owner.storm.kafka1;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.owner.storm.kafka.KafkaSpoutConfig;
import com.owner.storm.kafka.MessageMetaInfo;
import com.owner.storm.kafka.serializer.SerialType;
import com.owner.storm.kafka.serializer.Serializer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by admin on 2017/4/27.
 */
public class PartitionConsumer
{
	private static Logger LOG = LoggerFactory.getLogger(PartitionConsumer.class);

	private ConsumerFactory factory;
	private KafkaConsumer kafkaConsumer;
	private Map conf;
	private KafkaSpoutConfig config;
	private int partition;
	private String topic;
	private SerialType serialType = SerialType.nlmsg;
	private Serializer serializer;

	private long emittedOffset;
	private long lastCommittedOffset;

	private LinkedList<ConsumerRecord<String, byte[]>> emittedMessages = new LinkedList<ConsumerRecord<String, byte[]>>();
	private SortedSet<Long> pendingOffsets = new TreeSet<Long>();
	private SortedSet<Long> failedOffsets = new TreeSet<Long>();

	enum EmitState
	{
		EMIT_MORE,
		EMIT_END,
		EMIT_NONE
	}

	public PartitionConsumer(Map conf, KafkaSpoutConfig config, String topic, int partition)
	{
		this.conf = conf;
		this.config = config;
		this.partition = partition;
		this.topic = topic;
		kafkaConsumer = new KafkaConsumer(config, topic, partition);

		if (StringUtils.isNotEmpty(config.serialType))
		{
			SerialType _serialType = serialType.getType(config.serialType);
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

		try
		{
			if (config.fromBeginning)
			{
				kafkaConsumer.seekToBeginning();
			}
		}
		catch (Exception e)
		{

		}
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
				ConsumerRecord<String, byte[]> toEmitMsg = emittedMessages.pollFirst();
				if (toEmitMsg == null)
				{
					break;
				}
				collector.emit(toTuple(toEmitMsg.value()), new MessageMetaInfo(partition, toEmitMsg.offset()));
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

		ConsumerRecords<String, byte[]> messageSet = null;
		try
		{
			long start = System.currentTimeMillis();
			long startOffset = emittedOffset;

			messageSet = kafkaConsumer.fetchMessages();

			if (null == messageSet)
			{
				return false;
			}

			int count = 0;
			for (ConsumerRecord<String, byte[]> message : messageSet)
			{
				count++;

				pendingOffsets.add(emittedOffset);
				emittedMessages.add(message);
				emittedOffset = message.offset() + 1;
			}

			long end = System.currentTimeMillis();

			if (LOG.isDebugEnabled())
			{
				LOG.debug(
						"fetch message from partition:" + partition + ", start offset:" + startOffset + ", latest offset:" + emittedOffset
								+ ", count:" + count
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
				kafkaConsumer.seek(emittedOffset);
			}
			long lastOffset = lastCompletedOffset();
			if (lastOffset != lastCommittedOffset)
			{
				lastCommittedOffset = lastOffset;
				OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(lastOffset);
				kafkaConsumer.commitState(offsetAndMetadata);
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

	private Values toTuple(byte[] _bytes)
	{
		Values _value = new Values();
		_value.add(serializer.unserialize(_bytes));
		return _value;
	}

	private byte[] toByteArray(ByteBuffer buffer)
	{
		byte[] ret = new byte[buffer.remaining()];
		buffer.get(ret, 0, ret.length);
		return ret;
	}
}
