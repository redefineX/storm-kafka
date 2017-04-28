package com.owner.storm.kafka;

/**
 * Created by admin on 2017/4/27.
 */
public class MessageMetaInfo
{
	private int partition;
	private long offset;

	public MessageMetaInfo(int partition, long offset)
	{
		this.partition = partition;
		this.offset = offset;
	}

	public int getPartition()
	{
		return partition;
	}

	public void setPartition(int partition)
	{
		this.partition = partition;
	}

	public long getOffset()
	{
		return offset;
	}

	public void setOffset(long offset)
	{
		this.offset = offset;
	}
}
