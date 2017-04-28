package com.owner.storm.kafka;

//import com.alibaba.jstorm.utils.JStormUtils;

import com.owner.storm.tool.StormUtils;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by admin on 2017/3/27.
 */
public class KafkaSpoutConfig implements Serializable
{
	public List<Host> brokers;
	public int numPartitions;
	public String topic;
	public String zkRoot;

	public List<Host> zkServers;
	private Map stormConfig;

	public int fetchMaxBytes = 256 * 1024;
	public int fetchWaitMaxMs = 10000;
	public int socketTimeoutMs = 30 * 1000;
	public int socketReceiveBufferBytes = 64 * 1024;
	public long startOffsetTime = -1;
	public boolean fromBeginning = false;
	public String clientId;
	public boolean resetOffsetIfOutOfRange = false;
	public long offsetUpdateIntervalMs = 2000;
	private Properties properties = null;
	private Map stormConf;
	public int batchSendCount = 1;

	public String serialType;
	public String transferType;

	public KafkaSpoutConfig()
	{
	}

	public KafkaSpoutConfig(Properties properties)
	{
		this.properties = properties;
	}

	public void config(Map stormConfig)
	{
		this.stormConf = stormConfig;
		topic = getConfig("kafka.topic", "jstorm");
		zkRoot = getConfig("jstorm.zookeeper.root", "/jstorm");

		String zkHosts = getConfig("kafka.zookeeper.hosts", "127.0.0.1:2181");
		zkServers = convertHost(zkHosts);
		String brokerHosts = getConfig("kafka.broker.hosts", "127.0.0.1:9092");
		brokers = convertHost(brokerHosts);

		numPartitions = StormUtils.parseInt(getConfig("kafka.broker.partitions"), 1);
		fetchMaxBytes = StormUtils.parseInt(getConfig("kafka.fetch.max.bytes"), 256 * 1024);
		fetchWaitMaxMs = StormUtils.parseInt(getConfig("kafka.fetch.wait.max.ms"), 10000);
		socketTimeoutMs = StormUtils.parseInt(getConfig("kafka.socket.timeout.ms"), 30 * 1000);
		socketReceiveBufferBytes = StormUtils.parseInt(getConfig("kafka.socket.receive.buffer.bytes"), 64 * 1024);
		fromBeginning = StormUtils.parseBoolean(getConfig("kafka.fetch.from.beginning"), false);
		startOffsetTime = StormUtils.parseInt(getConfig("kafka.start.offset.time"), -1);
		offsetUpdateIntervalMs = StormUtils.parseInt(getConfig("kafka.offset.update.interval.ms"), 2000);
		clientId = getConfig("kafka.client.id", "jstorm");
		batchSendCount = StormUtils.parseInt(getConfig("kafka.spout.batch.send.count"), 1);

		resetOffsetIfOutOfRange = StormUtils.parseBoolean(getConfig("kafka.reset.offset.ifoutofrange"), false);

		serialType = getConfig("kafka.message.serial.type", "nlmsg");
		transferType = getConfig("kafka.message.transfer.type", "no");
	}

	private String getConfig(String key)
	{
		return getConfig(key, null);
	}

	private String getConfig(String key, String defaultValue)
	{
		if (properties != null && properties.containsKey(key))
		{
			return properties.getProperty(key);
		}
		else if (stormConf.containsKey(key))
		{
			return String.valueOf(stormConf.get(key));
		}
		else
		{
			return defaultValue;
		}
	}

	public List<Host> convertHost(String hosts)
	{
		List<Host> hostList = new LinkedList<Host>();
		String[] hostArr = hosts.split(",");
		for (String s : hostArr)
		{
			Host host;
			String[] spec = s.split(":");
			if (spec.length == 2)
			{
				host = new Host(spec[0], Integer.parseInt(spec[1]));
			}
			else
			{
				throw new IllegalArgumentException("Invalid host specification: " + s);
			}
			hostList.add(host);
		}
		return hostList;
	}

	public List<Host> getBrokers()
	{
		return brokers;
	}

	public void setBrokers(List<Host> brokers)
	{
		this.brokers = brokers;
	}

	public int getNumPartitions()
	{
		return numPartitions;
	}

	public void setNumPartitions(int numPartitions)
	{
		this.numPartitions = numPartitions;
	}

	public String getTopic()
	{
		return topic;
	}

	public void setTopic(String topic)
	{
		this.topic = topic;
	}

	public String getZkRoot()
	{
		return zkRoot;
	}

	public void setZkRoot(String zkRoot)
	{
		this.zkRoot = zkRoot;
	}

	public List<Host> getZkServers()
	{
		return zkServers;
	}

	public void setZkServers(List<Host> zkServers)
	{
		this.zkServers = zkServers;
	}

	public Map getStormConfig()
	{
		return stormConfig;
	}

	public void setStormConfig(Map stormConfig)
	{
		this.stormConfig = stormConfig;
	}

	public int getFetchMaxBytes()
	{
		return fetchMaxBytes;
	}

	public void setFetchMaxBytes(int fetchMaxBytes)
	{
		this.fetchMaxBytes = fetchMaxBytes;
	}

	public int getFetchWaitMaxMs()
	{
		return fetchWaitMaxMs;
	}

	public void setFetchWaitMaxMs(int fetchWaitMaxMs)
	{
		this.fetchWaitMaxMs = fetchWaitMaxMs;
	}

	public int getSocketTimeoutMs()
	{
		return socketTimeoutMs;
	}

	public void setSocketTimeoutMs(int socketTimeoutMs)
	{
		this.socketTimeoutMs = socketTimeoutMs;
	}

	public int getSocketReceiveBufferBytes()
	{
		return socketReceiveBufferBytes;
	}

	public void setSocketReceiveBufferBytes(int socketReceiveBufferBytes)
	{
		this.socketReceiveBufferBytes = socketReceiveBufferBytes;
	}

	public long getStartOffsetTime()
	{
		return startOffsetTime;
	}

	public void setStartOffsetTime(long startOffsetTime)
	{
		this.startOffsetTime = startOffsetTime;
	}

	public boolean isFromBeginning()
	{
		return fromBeginning;
	}

	public void setFromBeginning(boolean fromBeginning)
	{
		this.fromBeginning = fromBeginning;
	}

	public String getClientId()
	{
		return clientId;
	}

	public void setClientId(String clientId)
	{
		this.clientId = clientId;
	}

	public boolean isResetOffsetIfOutOfRange()
	{
		return resetOffsetIfOutOfRange;
	}

	public void setResetOffsetIfOutOfRange(boolean resetOffsetIfOutOfRange)
	{
		this.resetOffsetIfOutOfRange = resetOffsetIfOutOfRange;
	}

	public long getOffsetUpdateIntervalMs()
	{
		return offsetUpdateIntervalMs;
	}

	public void setOffsetUpdateIntervalMs(long offsetUpdateIntervalMs)
	{
		this.offsetUpdateIntervalMs = offsetUpdateIntervalMs;
	}

	public Properties getProperties()
	{
		return properties;
	}

	public void setProperties(Properties properties)
	{
		this.properties = properties;
	}

	public Map getStormConf()
	{
		return stormConf;
	}

	public void setStormConf(Map stormConf)
	{
		this.stormConf = stormConf;
	}

	public int getBatchSendCount()
	{
		return batchSendCount;
	}

	public void setBatchSendCount(int batchSendCount)
	{
		this.batchSendCount = batchSendCount;
	}
}
