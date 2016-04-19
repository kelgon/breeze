package com.asiainfo.breeze.consumer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.asiainfo.breeze.util.InstanceHolder;

/**
 * This thread will continually take records from the blocking queue, and insert them to
 * the MongoDB collection configured.
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
				String record = InstanceHolder.queue.poll(5000, TimeUnit.MILLISECONDS);
				if(record == null)
					continue;
				
				log.debug("Record fetched: "+record);
				Document doc = Document.parse(record);
				long createTime = doc.getLong("brzRcdCrtTime");
				doc.remove("brzRcdCrtTime");
				String collection = "";
				if("day".equalsIgnoreCase(InstanceHolder.rollBy)) {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
					collection = InstanceHolder.collection + "_" + sdf.format(new Date(createTime));
				} else if("month".equalsIgnoreCase(InstanceHolder.rollBy)) {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
					collection = InstanceHolder.collection + "_" + sdf.format(new Date(createTime));
				} else {
					collection = InstanceHolder.collection;
				}
				InstanceHolder.recordMdb.getCollection(collection).insertOne(doc);
				log.debug("Record inserted to MongoDB collection \"" + collection + "\"");
			} catch(Throwable t) {
				log.error("Error occured in consumer thead", t);
			}
		}
	}

	/**
	 * Send a stop signal to this thread. The producer thread will quit after the completion of 
	 * current loop
	 */
	public void sigStop() {
		this.run = false;
	}
}
