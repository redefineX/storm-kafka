package com.owner.storm.kafka1;

import backtype.storm.task.TopologyContext;
import com.owner.storm.kafka.KafkaSpoutConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * kafka client
 * Created by admin on 2017/4/27.
 */
public class ConsumerFactory {
    private static Logger LOG = LoggerFactory.getLogger(ConsumerFactory.class);

    private KafkaSpoutConfig config;
    private TopologyContext context;
    private Map conf;

    private Map<Integer, PartitionConsumer> partitionConsumerMap;
    private List<PartitionConsumer> partitionConsumers;

    public ConsumerFactory(Map conf, KafkaSpoutConfig config, TopologyContext context) {
        this.conf = conf;
        this.config = config;
        this.context = context;
    }

    public void createConsumerPartition() {
        partitionConsumerMap = new HashMap<Integer, PartitionConsumer>();
        partitionConsumers = new LinkedList<PartitionConsumer>();
        int taskSize = context.getComponentTasks(context.getThisComponentId()).size();
        LOG.info("consumer factory, task index: {},task size: {},num partition: {}", context.getThisTaskIndex(), taskSize, config.numPartitions);
        for (int i = context.getThisTaskIndex(); i < config.numPartitions; i += taskSize) {
            PartitionConsumer partitionConsumer = new PartitionConsumer(conf, config, config.topic, i);
            partitionConsumers.add(partitionConsumer);
            partitionConsumerMap.put(i, partitionConsumer);
        }

    }

    public List<PartitionConsumer> getPartitionConsumers() {
        return partitionConsumers;
    }

    public PartitionConsumer getConsumer(int partition) {
        return partitionConsumerMap.get(partition);
    }

    public void removeConsumer(int partition) {
        PartitionConsumer partitionConsumer = partitionConsumerMap.get(partition);
        partitionConsumers.remove(partitionConsumer);
        partitionConsumerMap.remove(partition);
    }
}
