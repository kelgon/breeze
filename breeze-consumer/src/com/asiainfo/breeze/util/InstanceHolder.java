package com.asiainfo.breeze.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.asiainfo.breeze.consumer.ConsumerThread;
import com.asiainfo.breeze.consumer.ProducerThread;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class InstanceHolder {
	public static String defaultCollection;
	public static String defaultRollBy;
	
	public static KafkaConsumer<String, String> kc;
	public static MongoClient mClient;
	public static MongoDatabase recordMdb;
	public static MongoDatabase configMdb;
	public static BlockingQueue<String> queue;
	public static ProducerThread pt;
	public static Set<ConsumerThread> cThreads;
	public static Timer timer;
	public static Map<String, String> collectionMap = new HashMap<String, String>();
	
	public static int consumerNameCount;
}
