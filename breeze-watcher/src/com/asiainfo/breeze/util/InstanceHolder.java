package com.asiainfo.breeze.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class InstanceHolder {
	public static String defaultCollection;
	public static String rollBy;
	
	public static MongoClient mClient;
	public static MongoDatabase recordMdb;
	public static MongoDatabase monitorMdb;
	public static KafkaProducer<String, String> kp;
	
	public static int consumerNameCount;
	public static Map<String, String> mongoServers = new HashMap<String, String>();
	public static Map<String, String> zkNodes = new HashMap<String, String>();
}

