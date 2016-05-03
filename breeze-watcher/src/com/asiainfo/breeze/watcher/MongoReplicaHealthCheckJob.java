package com.asiainfo.breeze.watcher;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import com.asiainfo.breeze.util.AlarmSender;
import com.asiainfo.breeze.util.InstanceHolder;

public class MongoReplicaHealthCheckJob implements Job {
	private final static Logger log = Logger.getLogger(MongoReplicaHealthCheckJob.class);
	public void execute(JobExecutionContext context) {
		log.info("MongoReplicaHealthCheckJob started");
		try {
			Document rsStatus = InstanceHolder.mClient.getDatabase("admin").runCommand(new Document("replSetGetStatus", "1"));
			String master = "";
			for(Entry<String, String> e : InstanceHolder.mongoServers.entrySet()) {
				if("PRIMARY".equals(e.getValue())) {
					master = e.getKey();
					break;
				}
			}
			boolean hasMaster = false;
			for(Object o : ((ArrayList<?>)rsStatus.get("members"))) {
				Document doc = (Document)o;
				log.info("Mongo node [" + doc.getString("name") + "] status: [" + doc.getInteger("state") + ":" +doc.getString("stateStr") + "]");
				int state = doc.getInteger("state");
				if(state == 0 || state == 3 || state == 5 || state == 6 || state == 8 || state == 9 || state == 10) {
					AlarmSender.sendAlarm("Mongo node [" + doc.getString("name") + "] is in abnormal state: [" + doc.getString("stateStr")
							+ "]! Previous state: [" + InstanceHolder.mongoServers.get(doc.getString("name")) + "]");
				} else if(state == 1) {
					hasMaster = true;
					if(!doc.getString("name").equals(master)) {
						AlarmSender.sendAlarm("Mongo master has changed from [" + master + "] to [" + doc.getString("name") + "]!");
					}
				}
				InstanceHolder.mongoServers.put(doc.getString("name"), doc.getString("stateStr"));
			}
			if(!hasMaster) {
				AlarmSender.sendAlarm("[Critical!!!] Mongo replica set currently has no living master!");
			}
		} catch(Throwable t) {
			log.error("error occured in MongoReplicaHealthCheckJob", t);
			AlarmSender.sendAlarm("[breeze] Failed to get MongoReplica status! " + t.getMessage());
		}
		log.info("MongoReplicaHealthCheckJob done");
	}
}
