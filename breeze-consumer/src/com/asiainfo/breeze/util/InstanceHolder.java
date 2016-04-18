package com.asiainfo.breeze.util;

import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.asiainfo.breeze.consumer.ConsumerThread;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class InstanceHolder {
	public static String collection;
	public static String rollBy;
	
	public static KafkaConsumer<String, String> kc;
	public static MongoClient mClient;
	public static MongoDatabase recordMdb;
	public static MongoDatabase configMdb;
	public static BlockingQueue<String> queue;
	public static Set<ConsumerThread> cThreads;
}
