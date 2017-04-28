package com.owner.storm.tool;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import com.owner.storm.kafka.KafkaSpout;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by admin on 2017/4/1.
 */
public class SubmitTool
{
	private static Logger logger = LoggerFactory.getLogger(SubmitTool.class);

	public static void main(String[] args)
	{
		try
		{
			logger.debug("command args [{}]", Arrays.toString(args));

			Options options = new Options();

			Option option = new Option("n", "name", true, "the name of the topology");
			option.setRequired(true);
			options.addOption(option);

			option = new Option("f", "config-file", true, "specify a config file for the topology");
			option.setRequired(true);
			options.addOption(option);

			option = new Option("h", "help", false, "display help text");
			options.addOption(option);

			CommandLineParser parser = new GnuParser();
			CommandLine commandLine = parser.parse(options, args);

			if (commandLine.hasOption('h'))
			{
				new HelpFormatter().printHelp("Jstorm topology", options, true);
				return;
			}

			String topoName = commandLine.getOptionValue("n");
			String configName = commandLine.getOptionValue("f");

			TopologyBuilder builder = new TopologyBuilder();

			builder.setSpout("kafka-spout", new KafkaSpout(), 5);
			builder.setBolt("count-bolt", new CountBolt(), 1).shuffleGrouping("kafka-spout");

			submitTopology(topoName, configName, builder.createTopology());
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void submitTopology(String topoName, String config, StormTopology topology)
			throws Exception
	{
		Yaml yaml = new Yaml();
		logger.info(config);
		Map<String, Object> yamlConfig = (Map<String, Object>) yaml.load(new InputStreamReader(new FileInputStream(config)));

		if (null == yamlConfig)
		{
			yamlConfig = new HashMap<String, Object>();
		}

		Config conf = new Config();
		conf.putAll(yamlConfig);

		conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
		//缓冲区存储消息的最大数量，默认为8（个数，并且得是2的倍数）
		//conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 16);//1.0 移除
		//默认为1024（个数，并且得是2的倍数）
		conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
		/*
		 * receive buffer 接收来自 receive 线程的的数据，sent buffer 向 sent 线程发送数据
		 * 参数默认都是 1024（个数，并且得是2的倍数）
		 */
		conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);

		/* 通常情况下 spout 的发射速度会快于下游的 bolt 的消费速度，当下游的 bolt 还有 TOPOLOGY_MAX_SPOUT_PENDING 个 tuple 没有消费完时，
		 * spout 会停下来等待，该配置作用于 spout 的每个 task。
		 */
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 10000);

		conf.put("topology.backpressure.enable", true);

		/* 调整分配给每个 worker 的内存，关于内存的调节，上文已有描述 */
		//conf.put(Config.WORKER_HEAP_MEMORY_MB, 768);
		//conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 768);

		StormSubmitter.submitTopology(topoName, conf, topology);

		//LocalCluster cluster = new LocalCluster();
		//cluster.submitTopology(topoName, conf, topology);
	}
}
