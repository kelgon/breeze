package com.asiainfo.breeze.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;

import com.asiainfo.breeze.util.InstanceHolder;

/**
 * This thread will continually pull records from Kafka and offer them to a blocking queue,
 * where consumer threads take their tasks from.
 * @author kelgon
 *
 */
public class ProducerThread extends Thread{
	private static final Logger log =  Logger.getLogger(ProducerThread.class);
	private boolean run = true;
	
	public void run() {
		log.info("Producer thread started");
		while(run) {
			try {
				log.info("Fetching records from Kafka...");
				ConsumerRecords<String, String> records = InstanceHolder.kc.poll(5000);
				log.info(records.count() + " new records fetched");
				for(ConsumerRecord<String, String> record : records) {
					InstanceHolder.queue.put(record.value());
				}
				log.info("added " + records.count() + " records to the queue, queue length: " + InstanceHolder.queue.size());
				if(records.count() > 0)
					InstanceHolder.kc.commitSync();
				Thread.sleep(1000);
			} catch(Throwable t) {
				log.error("Error occured in producer thead", t);
			}
		}
		log.warn("Producer thread ended");
	}
	
	/**
	 * Send a stop signal to this thread. The producer thread will quit after the completion of 
	 * current loop
	 */
	public void sigStop() {
		this.run = false;
	}
}
