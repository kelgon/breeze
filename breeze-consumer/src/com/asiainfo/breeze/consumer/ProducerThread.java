package com.asiainfo.breeze.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;

import com.asiainfo.breeze.util.InstanceHolder;

/**
 * 此线程会持续从Kafka集群拉取消息，置入阻塞队列中
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
				//从Kafka集群拉取消息，如果5秒内没有任何消息，则继续下一循环
				ConsumerRecords<String, String> records = InstanceHolder.kc.poll(5000);
				log.info(records.count() + " new records fetched");
				
				//将拉取到的消息写入阻塞队列
				for(ConsumerRecord<String, String> record : records) {
					InstanceHolder.queue.put(record.value());
				}
				log.info("added " + records.count() + " records to the queue, queue length: " + InstanceHolder.queue.size());
				
				//向Kafka集群提交offset
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
	 * 向此线程发送中止信号，线程会在完成当前循环后退出
	 */
	public void sigStop() {
		this.run = false;
	}
}
