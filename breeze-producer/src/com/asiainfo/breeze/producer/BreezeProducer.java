package com.asiainfo.breeze.producer;

import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class BreezeProducer {
	private static final Log log = LogFactory.getLog(BreezeProducer.class);
	private static KafkaProducer<String, String> kp = null;
	
	public static synchronized void init() {
		if(kp == null) {
			InputStream is = BreezeProducer.class.getClassLoader().getResourceAsStream("breeze-kafka.properties");
			Properties props = new Properties();
			try {
				props.load(is);
				kp = new KafkaProducer<String, String>(props);
			} catch(Throwable t) {
				log.error("Initialize KafkaProducerClient failed", t);
				kp = null;
			}
		}
	}
	
	/**
	 * Send a POJO record to Kafka cluster
	 * @param topic The topic of Kafka will be apended to
	 * @param key Key of the record, useful for load balancing among partitions
	 * @param o The record to be sent, must be POJO
	 * @param collection witch mongodb collection should this record append to
	 * @param createTime The creation-time of the record(Accurate to milliseconds)
	 * @param callback The code to execute when the request is complete
	 */
	public void send(String topic, String key, Object o, String collection, Date createTime, Callback callback) {
		if(kp == null) {
			init();
		}
		JSONObject obj = (JSONObject)JSON.toJSON(o);
		if(createTime != null)
			obj.put("brzRcdCrtTime", createTime.getTime());
		obj.put("bzRctCollection", collection);
		String objString = obj.toJSONString();
		kp.send(new ProducerRecord<String, String>(topic, key, objString), callback);
	}
	

	/**
	 * Send a POJO record to a single-node Kafka (Not recommend)
	 * @param topic The topic of Kafka will be apended to
	 * @param o The record to be sent, must be POJO
	 * @param collection witch mongodb collection should this record append to
	 * @param createTime The creation-time of the record(Accurate to milliseconds)
	 * @param callback The code to execute when the request is complete
	 */
	public void send(String topic, Object o, String collection, Date createTime, Callback callback) {
		if(kp == null) {
			init();
		}
		JSONObject obj = (JSONObject)JSON.toJSON(o);
		if(createTime != null)
			obj.put("brzRcdCrtTime", createTime.getTime());
		obj.put("brzRcdCollection", collection);
		String objString = obj.toJSONString();
		kp.send(new ProducerRecord<String, String>(topic, objString), callback);
	}
}
