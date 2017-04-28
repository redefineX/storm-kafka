package com.owner.storm.kafka2;

import backtype.storm.spout.SpoutOutputCollector;
import com.google.common.collect.ImmutableMap;
import com.owner.storm.kafka.KafkaSpoutConfig;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private long emittedOffset;
	private long lastCommittedOffset;

	private LinkedList<ConsumerRecord> emittedMessages = new LinkedList<ConsumerRecord>();
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

			List<Object> bytesList = new ArrayList<Object>();
			while (true)
			{
				ConsumerRecord toEmitMsg = emittedMessages.pollFirst();
				if (toEmitMsg == null)
				{
					break;
				}
				bytesList.add(toEmitMsg.value());
				count++;
				if (count >= config.batchSendCount)
				{
					break;
				}
			}
			if (CollectionUtils.isNotEmpty(bytesList))
			{
				//collector.emit(bytesList);
				LOG.info("--------------------{}", bytesList.size());
			}
			else
			{
				Thread.sleep(2000L);
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

		ConsumerRecords<String, String> messageSet = null;
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
			for (ConsumerRecord message : messageSet)
			{
				count++;

				//pendingOffsets.add(emittedOffset);
				emittedMessages.add(message);
				emittedOffset = message.offset() + 1;
			}

			long end = System.currentTimeMillis();

			//			if (LOG.isDebugEnabled())
			//			{
			//				LOG.debug(
			//						"fetch message from partition:" + partition + ", start offset:" + startOffset + ", latest offset:" + emittedOffset
			//								+ ", count:" + count
			//								+ ", time:" + (end - start));
			//			}
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

}
