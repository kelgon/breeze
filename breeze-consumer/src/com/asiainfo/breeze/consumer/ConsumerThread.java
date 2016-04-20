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
				log.info("Taking record from queue...");
				//take a task from blocking queue, if no tasks to take in 5 seconds, continue next loop
				String record = InstanceHolder.queue.poll(5000, TimeUnit.MILLISECONDS);
				if(record == null)
					continue;

				log.info("Record fetched");
				log.debug("Record fetched: "+record);
				
				//get record creation time and collection name
				Document doc = Document.parse(record);
				long createTime = doc.getLong("brzRcdCrtTime");
				doc.remove("brzRcdCrtTime");
				String collection;
				Object c = doc.get("brzRcdCollection");
				doc.remove("brzRcdCollection");
				//if no collection name, set to default
				if(c == null)
					collection = InstanceHolder.defaultCollection;
				else {
					if("".equals(c.toString()))
						collection = InstanceHolder.defaultCollection;
					else
						collection = c.toString();
				}
				//add date postfix to collection name
				if("day".equalsIgnoreCase(InstanceHolder.rollBy)) {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
					collection = collection + "_" + sdf.format(new Date(createTime));
				} else if("month".equalsIgnoreCase(InstanceHolder.rollBy)) {
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
	 * Send a stop signal to this thread. The producer thread will quit after the completion of 
	 * current loop
	 */
	public void sigStop() {
		this.run = false;
	}
}
