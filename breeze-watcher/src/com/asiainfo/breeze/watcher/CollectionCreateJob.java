package com.asiainfo.breeze.watcher;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import com.asiainfo.breeze.util.AlarmSender;
import com.asiainfo.breeze.util.InstanceHolder;

/**
 * Job to create new collections and indexes for the rollBy mechanism
 * @author kelgon
 *
 */
public class CollectionCreateJob implements Job{
	private static final Logger log = Logger.getLogger(CollectionCreateJob.class);
	
	public void execute(JobExecutionContext context) {
		try {
			log.info("loading breeze-watcher.properties...");
			InputStream is = CollectionCreateJob.class.getClassLoader().getResourceAsStream("breeze-watcher.properties");
			Properties props = new Properties();
			props.load(is);
			log.info("creating new collections and indexes...");
			String collections = props.getProperty("record.collections");
			if("".equals(collections) || collections == null) {
				log.error("record.collections must not be null or empty!");
				AlarmSender.sendAlarm("[breeze-watcher] collection创建/删除任务发生异常: record.collections未配置");
				return;
			}
			for(String collection : collections.split(",")) {
				String[] c = collection.split(":");
				if("day".equalsIgnoreCase(c[1])) {
					Date tommorrow = new Date(new Date().getTime() + 24*3600*1000);
					SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
					String newCollection = c[0] + "_" + sdf.format(tommorrow);
					InstanceHolder.recordMdb.createCollection(newCollection);
					log.info("created collection: " + newCollection);
					String indexes = props.getProperty(c[0]+".indexes");
					if(!"".equals(indexes) && indexes != null) {
						for(String index : indexes.split(",")) {
							String i = InstanceHolder.recordMdb.getCollection(newCollection).createIndex(new Document(index,1));
							log.info("created index: " + newCollection + "." + i);
						}
					}
				} else if("month".equalsIgnoreCase(c[1])) {
					//每月第20天创建下个月的collection
					Calendar ca = Calendar.getInstance();
					if(ca.get(Calendar.DAY_OF_MONTH) == 20) {
						ca.add(Calendar.MONTH, 1);
						Date nextMonth = ca.getTime();
						SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
						String newCollection = c[0] + "_" + sdf.format(nextMonth);
						InstanceHolder.recordMdb.createCollection(newCollection);
						log.info("created collection: " + newCollection);
						String indexes = props.getProperty(c[0]+".indexes");
						if(!"".equals(indexes) && indexes != null) {
							for(String index : indexes.split(",")) {
								String i = InstanceHolder.recordMdb.getCollection(newCollection).createIndex(new Document(index,1));
								log.info("created index: " + newCollection + "." + i);
							}
						}
					}
				}
			}
		} catch(Throwable t) {
			log.error("error occurred in CollectionCreateJob!", t);
			AlarmSender.sendAlarm("[breeze-watcher] collection创建/删除任务发生异常: " + t.getMessage());
		}
	}
}
