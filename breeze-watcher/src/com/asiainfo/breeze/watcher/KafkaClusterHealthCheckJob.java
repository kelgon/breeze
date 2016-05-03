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

public class KafkaClusterHealthCheckJob implements Job {
	private static final Logger log = Logger.getLogger(KafkaClusterHealthCheckJob.class);
	public void execute(JobExecutionContext context) {
		log.info("KafkaClusterHealthCheckJob started");
		try {
			InputStream is = KafkaClusterHealthCheckJob.class.getClassLoader().getResourceAsStream("breeze-watcher.properties");
			Properties props = new Properties();
			props.load(is);
			String[] topics = props.getProperty("consumer.kafkaTopics").split(",");
			for(String topic : topics) {
				int replicaCount = Integer.parseInt(props.getProperty(topic+".replicaCount"));
				List<PartitionInfo> partitions = InstanceHolder.kp.partitionsFor(topic);
				for(PartitionInfo p : partitions) {
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
					if(p.replicas().length != replicaCount)
						AlarmSender.sendAlarm("Kafka topic["+topic+"] on partition"+p.partition()+" has unavailable replica! replica:"+replicas.toString()+" isr:"+isr.toString());
					if(p.inSyncReplicas().length != replicaCount)
						AlarmSender.sendAlarm("Kafka topic["+topic+"] on partition"+p.partition()+" has out-of-sync replica! replica:"+replicas.toString()+" isr:"+isr.toString());
					if(p.leader() == null || p.leader().id() == -1)
						AlarmSender.sendAlarm("[Critical!!!] Kafka topic["+p.topic()+"] on partition"+p.partition()+" has no available leader!");
					log.info("[partition:"+p.partition()+"][topic:"+topic+"] replicas:"+replicas.toString()+" leader:"+p.leader()+" isr:"+isr.toString());
				}
			}
			log.info("KafkaClusterHealthCheckJob done");
		} catch(Throwable t) {
			log.error("error occured in KafkaClusterHealthCheckJob", t);
			AlarmSender.sendAlarm("[breeze] Failed to get KafkaCluster status! " + t.toString());
		}
	}
}
