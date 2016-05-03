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
	 * 向Kafka集群(或单点服务)推送一个POJO对象
	 * @param topic 记录所属的topic
	 * @param key 记录的key，用于集群broker的负载均衡，如果目标是Kafka单点服务，此参数请传null
	 * @param o 记录对象，必须是POJO
	 * @param collection 此记录对应MongoDB中的collection名
	 * @param createTime 此记录的产生时间（精确到毫秒）
	 * @param callback 推送完成后的回调逻辑
	 */
	public void send(String topic, String key, Object o, String collection, Date createTime, Callback callback) {
		if(kp == null) {
			init();
		}
		JSONObject obj = (JSONObject)JSON.toJSON(o);
		if(createTime != null)
			obj.put("brzRcdCrtTime", createTime.getTime());
		obj.put("brzRcdCollection", collection);
		String objString = obj.toJSONString();
		if(key == null)
			kp.send(new ProducerRecord<String, String>(topic, objString), callback);
		else
			kp.send(new ProducerRecord<String, String>(topic, key, objString), callback);
	}
	
}
