package com.asiainfo.breeze.consumer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.asiainfo.breeze.util.InstanceHolder;

/**
 * 此线程会持续从阻塞队列中读取记录，并将其持久化至MongoDB中
 * @author kelgon
 *
 */
public class ConsumerThread extends Thread {
	private static final Logger log = Logger.getLogger(ConsumerThread.class);
	private boolean run = true;
	
	public void run() {
		log.info(this.getName() + " started");
		while(run) {
			try {
				log.debug("Taking record from queue...");
				//从阻塞队列中读取记录，如果5秒内未取到记录，则继续下一循环
				String record = InstanceHolder.queue.poll(5000, TimeUnit.MILLISECONDS);
				if(record == null)
					continue;

				log.info("Record fetched");
				log.debug("Record fetched: "+record);
				
				//从记录对象中获取目标collection名与记录生成时间
				Document doc = Document.parse(record);
				String collection,rollBy;
				Object c = doc.get("brzRcdCollection");
				doc.remove("brzRcdCollection");
				if(c == null) {
					collection = InstanceHolder.defaultCollection;
					rollBy = InstanceHolder.defaultRollBy;
				}
				else {
					if("".equals(c.toString())) {
						collection = InstanceHolder.defaultCollection;
						rollBy = InstanceHolder.defaultRollBy;
					}
					else {
						collection = c.toString();
						rollBy = InstanceHolder.collectionMap.get(collection);
					}
				}
				Long createTime;
				Object cTime = doc.get("brzRcdCrtTime");
				doc.remove("brzRcdCrtTime");
				try {
					createTime = (Long)cTime;
				} catch(Throwable t) {
					createTime = null;
				}
				
				//在collection名后追加分片时间戳
				if("day".equalsIgnoreCase(rollBy) && createTime != null) {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
					collection = collection + "_" + sdf.format(new Date(createTime));
				} else if("month".equalsIgnoreCase(rollBy) && createTime != null) {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
					collection = collection + "_" + sdf.format(new Date(createTime));
				}
				
				//insert record to mongodb
				InstanceHolder.recordMdb.getCollection(collection).insertOne(doc);
				log.info("Record inserted to MongoDB collection \"" + collection + "\"");
			} catch(Throwable t) {
				log.error("Error occured in consumer thead", t);
			}
		}
	}

	/**
	 * 向此线程发送中止信号，线程会在完成当前循环后退出
	 */
	public void sigStop() {
		this.run = false;
	}
}
