package com.owner.storm.kafka;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.ProtectACLCreateModePathAndBytesable;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by admin on 2017/3/27.
 */
public class ZkState
{
	private static Logger logger = LoggerFactory.getLogger(ZkState.class);
	private CuratorFramework curators;
	private ExecutorService pool;

	public ZkState(Map stateConf, KafkaSpoutConfig config)
	{
		curators = newCurator(stateConf, config);
	}

	private CuratorFramework newCurator(Map conf, KafkaSpoutConfig config)
	{
		Preconditions.checkArgument(CollectionUtils.isNotEmpty(config.getZkServers()), "配置异常，zk连接串不能为空，进程启动失败");

		if (curators == null)
		{
			try
			{
				pool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("zookeeper 监控处理线程_%d").setDaemon(true).build());

				String serverPorts = "";
				List<Host> zkServers = config.zkServers;
				for (Host server : zkServers)
				{
					serverPorts = serverPorts + server.getHost() + ":" + server.getPort() + ",";
				}
				curators = CuratorFrameworkFactory.newClient(serverPorts, new RetryNTimes(2000, 20000));
				curators.start();
			}
			catch (Exception e)
			{
				logger.error("InterruptedException happend!!!", e);
			}
		}
		return curators;
	}

	public CuratorFramework getCurators()
	{
		assert curators != null;
		return curators;
	}

	public void writeJSON(String path, Map<Object, Object> data)
	{
		logger.debug("Writing " + path + " the data " + data.toString());
		writeBytes(path, JSON.toJSONString(data).getBytes(Charset.forName("UTF-8")));
	}

	public void writeBytes(String path, byte[] bytes)
	{
		try
		{
			if (curators.checkExists().forPath(path) == null)
			{
				CreateBuilder builder = curators.create();
				ProtectACLCreateModePathAndBytesable<String> createAble = (ProtectACLCreateModePathAndBytesable<String>) builder
						.creatingParentsIfNeeded();
				createAble.withMode(CreateMode.PERSISTENT).forPath(path, bytes);
			}
			else
			{
				curators.setData().forPath(path, bytes);
			}
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public Map<Object, Object> readJSON(String path)
	{
		try
		{
			byte[] b = readBytes(path);
			if (b == null)
				return null;
			return (Map<Object, Object>) JSON.parse(new String(b, "UTF-8"));
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public byte[] readBytes(String path)
	{
		try
		{
			if (curators.checkExists().forPath(path) != null)
			{
				return curators.getData().forPath(path);
			}
			else
			{
				return null;
			}
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public void close()
	{
		curators.close();
		curators = null;
	}

}
