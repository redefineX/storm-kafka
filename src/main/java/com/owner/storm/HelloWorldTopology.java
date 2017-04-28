package com.owner.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by admin on 2017/2/13.
 */
public class HelloWorldTopology
{
	public static void main ( String[] args ) throws Exception
	{
		TopologyBuilder builder = new TopologyBuilder ();
		builder.setSpout ("randomHelloWorld", new HelloWorldSpout (), 3);
		builder.setBolt ("HelloWorldBolt", new HelloWorldBolt (), 1).shuffleGrouping ("randomHelloWorld");
		Config conf = new Config ();
		conf.setDebug (true);

		//缓冲区存储消息的最大数量，默认为8（个数，并且得是2的倍数）
		//conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 16);//1.0 移除
		//默认为1024（个数，并且得是2的倍数）
		conf.put (Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
		/*
		 * receive buffer 接收来自 receive 线程的的数据，sent buffer 向 sent 线程发送数据
		 * 参数默认都是 1024（个数，并且得是2的倍数）
		 */
		conf.put (Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		conf.put (Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);

		/* 通常情况下 spout 的发射速度会快于下游的 bolt 的消费速度，当下游的 bolt 还有 TOPOLOGY_MAX_SPOUT_PENDING 个 tuple 没有消费完时，
		 * spout 会停下来等待，该配置作用于 spout 的每个 task。
		 */
		conf.put (Config.TOPOLOGY_MAX_SPOUT_PENDING, 10000);

		conf.put ("topology.backpressure.enable", true);

		/* 调整分配给每个 worker 的内存，关于内存的调节，上文已有描述 */
		//conf.put(Config.WORKER_HEAP_MEMORY_MB, 768);
		//conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 768);

		//ConfigExtension.setUserDefinedLog4jConf(conf, "log4j.properties");
		if (args != null && args.length > 0)
		{
			conf.setNumWorkers (1);
			StormSubmitter.submitTopology (args[0], conf, builder.createTopology ());
		}
		else
		{
			String topoName = "SequenceTest";
			LocalCluster cluster = new LocalCluster ();
			conf.put (Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
			cluster.submitTopology (topoName, conf, builder.createTopology ());

			try
			{
				Thread.sleep (60000);
			}
			catch (Exception e)
			{
			}

			//结束拓扑
			cluster.killTopology (topoName);
			cluster.shutdown ();
		}
	}
}
