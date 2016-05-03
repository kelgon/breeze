package com.asiainfo.breeze.watcher;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import com.asiainfo.breeze.util.AlarmSender;
import com.asiainfo.breeze.util.InstanceHolder;

/**
 * Kafka集群健康监测任务
 * @author kelgon
 *
 */
public class KafkaClusterHealthCheckJob implements Job {
	private static final Logger log = Logger.getLogger(KafkaClusterHealthCheckJob.class);
	public void execute(JobExecutionContext context) {
		log.info("KafkaClusterHealthCheckJob started");
		try {
			InputStream is = KafkaClusterHealthCheckJob.class.getClassLoader().getResourceAsStream("breeze-watcher.properties");
			Properties props = new Properties();
			props.load(is);
			//获取监控的topic
			String[] topics = props.getProperty("consumer.kafkaTopics").split(",");
			for(String topic : topics) {
				int replicaCount = Integer.parseInt(props.getProperty(topic+".replicaCount"));
				//获得该topic的metadata
				List<PartitionInfo> partitions = InstanceHolder.kp.partitionsFor(topic);
				for(PartitionInfo p : partitions) {
					//获取该topic当前分区的状态
					StringBuffer replicas = new StringBuffer();
					StringBuffer isr = new StringBuffer();
					if(p.replicas().length == 0) {
						replicas.append("none");
					} else {
						for(Node n : p.replicas()) {
							replicas.append(n.idString()).append(",");
						}
						replicas.deleteCharAt(replicas.length() - 1);
						if(p.inSyncReplicas().length == 0) {
							isr.append("none");
						} else {
							for(Node n : p.inSyncReplicas()) {
								isr.append(n.idString()).append(",");
							}
							isr.deleteCharAt(isr.length() - 1);
						}
					}
					//如果分区replica数少于定义数，告警
					if(p.replicas().length != replicaCount)
						AlarmSender.sendAlarm("Kafka topic["+topic+"] on partition"+p.partition()+" 存在不可用的replica! replica:"+replicas.toString()+" isr:"+isr.toString());
					//如果分区in-sync replica数少于定义数，告警
					if(p.inSyncReplicas().length != replicaCount)
						AlarmSender.sendAlarm("Kafka topic["+topic+"] on partition"+p.partition()+" 存在不同步的replica! replica:"+replicas.toString()+" isr:"+isr.toString());
					//如果分区无有效leader，告警
					if(p.leader() == null || p.leader().id() == -1)
						AlarmSender.sendAlarm("[严重告警] Kafka topic["+p.topic()+"] on partition"+p.partition()+" 当前没有可用Leader！");
					log.info("[partition:"+p.partition()+"][topic:"+topic+"] replicas:"+replicas.toString()+" leader:"+p.leader()+" isr:"+isr.toString());
				}
			}
			log.info("KafkaClusterHealthCheckJob done");
		} catch(Throwable t) {
			log.error("error occured in KafkaClusterHealthCheckJob", t);
			AlarmSender.sendAlarm("[breeze-watcher] 获取Kafka集群状态失败：" + t.toString());
		}
	}
}
