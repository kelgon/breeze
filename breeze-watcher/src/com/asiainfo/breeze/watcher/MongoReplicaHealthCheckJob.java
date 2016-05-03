package com.asiainfo.breeze.watcher;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import com.asiainfo.breeze.util.AlarmSender;
import com.asiainfo.breeze.util.InstanceHolder;

/**
 * MongoDB集群健康监测任务
 * @author kelgon
 *
 */
public class MongoReplicaHealthCheckJob implements Job {
	private final static Logger log = Logger.getLogger(MongoReplicaHealthCheckJob.class);
	public void execute(JobExecutionContext context) {
		log.info("MongoReplicaHealthCheckJob started");
		try {
			//获取MongoDB集群状态
			Document rsStatus = InstanceHolder.mClient.getDatabase("admin").runCommand(new Document("replSetGetStatus", "1"));
			String master = "";//上一次检查时的Master节点
			for(Entry<String, String> e : InstanceHolder.mongoServers.entrySet()) {
				if("PRIMARY".equals(e.getValue())) {
					master = e.getKey();
					break;
				}
			}
			boolean hasMaster = false;
			//遍历所有节点，检查状态
			for(Object o : ((ArrayList<?>)rsStatus.get("members"))) {
				Document doc = (Document)o;
				log.info("Mongo node [" + doc.getString("name") + "] status: [" + doc.getInteger("state") + ":" +doc.getString("stateStr") + "]");
				int state = doc.getInteger("state");
				//如果节点状态异常，告警
				if(state == 0 || state == 3 || state == 5 || state == 6 || state == 8 || state == 9 || state == 10) {
					AlarmSender.sendAlarm("Mongo node [" + doc.getString("name") + "] is in abnormal state: [" + doc.getString("stateStr")
							+ "]! Previous state: [" + InstanceHolder.mongoServers.get(doc.getString("name")) + "]");
				} else if(state == 1) {
					hasMaster = true;
					//如果当前Master与上一次检查时的Master不同，告警
					if(!doc.getString("name").equals(master)) {
						AlarmSender.sendAlarm("Mongo master has changed from [" + master + "] to [" + doc.getString("name") + "]!");
					}
				}
				//更新MongoDB节点状态Map
				InstanceHolder.mongoServers.put(doc.getString("name"), doc.getString("stateStr"));
			}
			//如果当前无有效Master，告警
			if(!hasMaster) {
				AlarmSender.sendAlarm("[严重告警] MongoDB集群中当前没有可用Master！");
			}
		} catch(Throwable t) {
			log.error("error occured in MongoReplicaHealthCheckJob", t);
			AlarmSender.sendAlarm("[breeze-watcher] 获取MongoDB集群状态失败：" + t.getMessage());
		}
		log.info("MongoReplicaHealthCheckJob done");
	}
}
