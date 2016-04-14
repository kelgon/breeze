package com.asiainfo.breeze.producer;

import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.alibaba.fastjson.JSON;

public class BreezeProducer {
	private static final Log log = LogFactory.getLog(BreezeProducer.class);
	private static KafkaProducer<String, String> kp = null;
	
	public static synchronized void init() {
		if(kp == null) {
			InputStream is = BreezeProducer.class.getClassLoader().getResourceAsStream("breeze.properties");
			Properties props = new Properties();
			try {
				props.load(is);
				kp = new KafkaProducer<String, String>(props);
			} catch(Throwable t) {
				log.error("初始化KafkaProducerClient失败", t);
				kp = null;
			}
		}
	}
	
	/**
	 * 向Kafka集群发送一个结构化日志对象
	 * @param topic 对象消息所属的topic
	 * @param key 要发送对象的key，用于Kafka集群节点间的负载均衡，所以需尽可能确保key的分散度
	 * @param o 要发送的结构化日志，必须为POJO对象
	 * @param callback 发送完成后的回调逻辑
	 */
	public void send(String topic, String key, Object o, Callback callback) {
		if(kp == null) {
			init();
		}
		String objString = JSON.toJSONString(o);
		kp.send(new ProducerRecord<String, String>(topic, key, objString), callback);
	}
	

	/**
	 * 向Kafka服务（单点）发送一个结构化日志对象（不推荐）
	 * @param topic 对象消息所属的topic
	 * @param o 要发送的结构化日志，必须为POJO对象
	 * @param callback 发送完成后的回调逻辑
	 */
	public void send(String topic, Object o, Callback callback) {
		if(kp == null) {
			init();
		}
		String objString = JSON.toJSONString(o);
		kp.send(new ProducerRecord<String, String>(topic, objString), callback);
	}
}
