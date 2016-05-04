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
 * 定时创建collection与索引，并删除过期collection
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
			String collections = props.getProperty("breeze.recordCollections");
			if("".equals(collections) || collections == null) {
				log.error("breeze.recordCollections must not be null or empty!");
				AlarmSender.sendAlarm("[breeze-watcher] collection创建/删除任务发生异常: record.collections未配置");
				return;
			}
			
			//创建下一天的collection与索引
			for(String collection : collections.split(",")) {
				String rollBy = props.getProperty(collection+".rollBy");
				if("day".equalsIgnoreCase(rollBy)) {
					Date tommorrow = new Date(new Date().getTime() + 24*3600*1000);
					SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
					String newCollection = collection + "_" + sdf.format(tommorrow);
					InstanceHolder.recordMdb.createCollection(newCollection);
					log.info("created collection: " + newCollection);
					String indexes = props.getProperty(collection+".index");
					if(!"".equals(indexes) && indexes != null) {
						for(String index : indexes.split(",")) {
							String i = InstanceHolder.recordMdb.getCollection(newCollection).createIndex(new Document(index,1));
							log.info("created index: " + newCollection + "." + i);
						}
					}
				} else if("month".equalsIgnoreCase(rollBy)) {
					//每月第20天创建下个月的collection
					Calendar ca = Calendar.getInstance();
					if(ca.get(Calendar.DAY_OF_MONTH) == 20) {
						ca.add(Calendar.MONTH, 1);
						Date nextMonth = ca.getTime();
						SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
						String newCollection = collection + "_" + sdf.format(nextMonth);
						InstanceHolder.recordMdb.createCollection(newCollection);
						log.info("created collection: " + newCollection);
						String indexes = props.getProperty(collection+".index");
						if(!"".equals(indexes) && indexes != null) {
							for(String index : indexes.split(",")) {
								String i = InstanceHolder.recordMdb.getCollection(newCollection).createIndex(new Document(index,1));
								log.info("created index: " + newCollection + "." + i);
							}
						}
					}
				}
			}

			//删除过期collection
			for(String collection : collections.split(",")) {
				String rollBy = props.getProperty(collection+".rollBy");
				String deleteAfter = props.getProperty(collection+".deleteAfter");
				if("".equals(deleteAfter) || deleteAfter == null)
					return;
				int deleteAfterI = Integer.parseInt(props.getProperty(collection+".deleteAfter"));
				if("day".equalsIgnoreCase(rollBy)) {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
					Calendar ca = Calendar.getInstance();
					ca.add(Calendar.DAY_OF_MONTH, -deleteAfterI);
					String collection2delete = collection + "_" + sdf.format(ca.getTime());
					InstanceHolder.recordMdb.getCollection(collection2delete).drop();
					log.info("collection dropped: " + collection2delete);
				} else if("month".equalsIgnoreCase(rollBy)) {
					//如按月切分，则每月第2天删除过期collection
					Calendar ca = Calendar.getInstance();
					if(ca.get(Calendar.DAY_OF_MONTH) == 2) {
						SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
						ca.add(Calendar.MONTH, -deleteAfterI);
						String collection2delete = collection + "_" + sdf.format(ca.getTime());
						InstanceHolder.recordMdb.getCollection(collection2delete).drop();
						log.info("collection dropped: " + collection2delete);
					}
				}
				
			}
		} catch(Throwable t) {
			log.error("error occurred in CollectionCreateJob!", t);
			AlarmSender.sendAlarm("[breeze-watcher] collection创建/删除任务发生异常: " + t.getMessage());
		}
	}
}
