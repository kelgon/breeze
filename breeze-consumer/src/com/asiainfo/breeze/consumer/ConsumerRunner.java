package com.asiainfo.breeze.consumer;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.asiainfo.breeze.util.InstanceHolder;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadConcern;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

public class ConsumerRunner {
	private static final Logger log = Logger.getLogger(ConsumerRunner.class);
	
	private static boolean initKafkaConsumer() {
		try {
			log.info("loading breeze-kafka.properties...");
			InputStream is = ConsumerRunner.class.getClassLoader().getResourceAsStream("breeze-kafka.properties");
			Properties props = new Properties();
			props.load(is);
			InstanceHolder.kc = new KafkaConsumer<String, String>(props);
			return true;
		} catch(Throwable t) {
			log.error("initializing kafka client failed", t);
			return false;
		}
	}
	
	private static boolean initMongo() {
		try {
			log.info("loading breeze-mongo.properties...");
			InputStream is = ConsumerRunner.class.getClassLoader().getResourceAsStream("breeze-mongo.properties");
			Properties props = new Properties();
			props.load(is);
			String servers = props.getProperty("mongo.servers");
			if("".equals(servers) || servers == null) {
				log.error("mongo.servers must not be null or empty!");
				return false;
			}
			List<ServerAddress> serverList = new ArrayList<ServerAddress>();
			for(String s : servers.split(",")) {
				String[] addr = s.split(":");
				ServerAddress sa = new ServerAddress(addr[0], Integer.parseInt(addr[1]));
				serverList.add(sa);
			}
			
			String recordDbName = props.getProperty("record.dbname");
			if("".equals(recordDbName) || recordDbName == null) {
				log.error("record.dbname must not be null or empty!");
				return false;
			}
			String recordCredentials = props.getProperty("record.credentials");
			String configDbName = props.getProperty("config.dbname");
			if("".equals(configDbName) || configDbName == null) {
				log.error("config.dbname must not be null or empty!");
				return false;
			}
			String configCredentials = props.getProperty("config.credentials");
			List<MongoCredential> mCreList = new ArrayList<MongoCredential>();
			if(!"".equals(recordCredentials) && recordCredentials != null) {
				String[] cre = recordCredentials.split(":");
				MongoCredential credential = MongoCredential.createMongoCRCredential(cre[0], recordDbName, cre[1].toCharArray());
				mCreList.add(credential);
			}
			if(!"".equals(configCredentials) && configCredentials != null) {
				String[] cre = configCredentials.split(":");
				MongoCredential credential = MongoCredential.createMongoCRCredential(cre[0], configDbName, cre[1].toCharArray());
				mCreList.add(credential);
			}
			
			Builder options = new MongoClientOptions.Builder();
			if(props.containsKey("mongo.connectionsPerHost"))
				options.connectionsPerHost(Integer.parseInt(props.getProperty("mongo.connectionsPerHost")));
			if(props.containsKey("mongo.connectTimeout"))
				options.connectTimeout(Integer.parseInt(props.getProperty("mongo.connectTimeout")));
			if(props.containsKey("mongo.heartbeatConnectTimeout"))
				options.heartbeatConnectTimeout(Integer.parseInt(props.getProperty("mongo.heartbeatConnectTimeout")));
			if(props.containsKey("mongo.heartbeatFrequency"))
				options.heartbeatFrequency(Integer.parseInt(props.getProperty("mongo.heartbeatFrequency")));
			if(props.containsKey("mongo.heartbeatSocketTimeout"))
				options.heartbeatSocketTimeout(Integer.parseInt(props.getProperty("mongo.heartbeatSocketTimeout")));
			if(props.containsKey("mongo.maxConnectionIdleTime"))
				options.connectTimeout(Integer.parseInt(props.getProperty("mongo.maxConnectionIdleTime")));
			if(props.containsKey("mongo.maxConnectionLifeTime"))
				options.maxConnectionLifeTime(Integer.parseInt(props.getProperty("mongo.maxConnectionLifeTime")));
			if(props.containsKey("mongo.maxWaitTime"))
				options.maxWaitTime(Integer.parseInt(props.getProperty("mongo.maxWaitTime")));
			if(props.containsKey("mongo.minConnectionsPerHost"))
				options.minConnectionsPerHost(Integer.parseInt(props.getProperty("mongo.minConnectionsPerHost")));
			if(props.containsKey("mongo.minHeartbeatFrequency"))
				options.minHeartbeatFrequency(Integer.parseInt(props.getProperty("mongo.minHeartbeatFrequency")));
			if(props.containsKey("mongo.readConcern")) {
				String readConcern = props.getProperty("mongo.readConcern");
				if("default".equalsIgnoreCase(readConcern))
					options.readConcern(ReadConcern.DEFAULT);
				if("local".equalsIgnoreCase(readConcern))
					options.readConcern(ReadConcern.LOCAL);
				if("majority".equalsIgnoreCase(readConcern))
					options.readConcern(ReadConcern.MAJORITY);
			}
			if(props.containsKey("mongo.readPreference")) {
				String readPreference = props.getProperty("mongo.readPreference");
				if("primary".equalsIgnoreCase(readPreference))
					options.readPreference(ReadPreference.primary());
				if("primaryPreferred".equalsIgnoreCase(readPreference))
					options.readPreference(ReadPreference.primaryPreferred());
				if("secondary".equalsIgnoreCase(readPreference))
					options.readPreference(ReadPreference.secondary());
				if("secondaryPreferred".equalsIgnoreCase(readPreference))
					options.readPreference(ReadPreference.secondaryPreferred());
				if("nearest".equalsIgnoreCase(readPreference))
					options.readPreference(ReadPreference.nearest());
			}
			if(props.containsKey("mongo.serverSelectionTimeout"))
				options.serverSelectionTimeout(Integer.parseInt(props.getProperty("mongo.serverSelectionTimeout")));
			if(props.containsKey("mongo.socketTimeout"))
				options.socketTimeout(Integer.parseInt(props.getProperty("mongo.socketTimeout")));
			if(props.containsKey("mongo.threadsAllowedToBlockForConnectionMultiplier"))
				options.threadsAllowedToBlockForConnectionMultiplier(Integer.parseInt(props.getProperty("mongo.threadsAllowedToBlockForConnectionMultiplier")));
			if(props.containsKey("mongo.writeConcern"))
				options.writeConcern(new WriteConcern(Integer.parseInt(props.getProperty("mongo.writeConcern"))));
			if(props.containsKey("mongo.socketKeepAlive"))
				options.socketKeepAlive(Boolean.parseBoolean(props.getProperty("mongo.socketKeepAlive")));
			if(props.containsKey("mongo.sslEnabled"))
				options.sslEnabled(Boolean.parseBoolean(props.getProperty("mongo.sslEnabled")));
			if(props.containsKey("mongo.sslInvalidHostNameAllowed"))
				options.sslInvalidHostNameAllowed(Boolean.parseBoolean(props.getProperty("mongo.sslInvalidHostNameAllowed")));
			
			log.info("initializing mongodb client...");
			InstanceHolder.mClient = new MongoClient(serverList, mCreList);
			InstanceHolder.recordMdb = InstanceHolder.mClient.getDatabase(recordDbName);
			InstanceHolder.configMdb = InstanceHolder.mClient.getDatabase(configDbName);
			return true;
		} catch(Throwable t) {
			log.error("initializing kafka client failed", t);
			return false;
		}
	}
	
	private static boolean initBreezeConsumer() {
		ProducerThread pt = new ProducerThread();
		Timer timer = new Timer();
		try {
			log.debug("loading breeze-consumer.properties...");
			InputStream is = ConsumerRunner.class.getClassLoader().getResourceAsStream("breeze-consumer.properties");
			Properties props = new Properties();
			props.load(is);
			String queueSize = props.getProperty("consumer.queueSize");
			if("".equals(queueSize) || queueSize == null) {
				log.error("consumer.queueSize must not be null or empty!");
				return false;
			}
			log.info("initializing blocking queue...");
			if("0".equals(queueSize))
				InstanceHolder.queue = new LinkedBlockingQueue<String>();
			else
				InstanceHolder.queue = new LinkedBlockingQueue<String>(Integer.parseInt(queueSize));
			String threadCount = props.getProperty("consumer.threadCount");
			if("".equals(threadCount) || threadCount == null) {
				log.error("consumer.threadCount must not be null or empty!");
				return false;
			}
			//
			String collection = props.getProperty("record.collectionName");
			if("".equals(collection) || collection == null) {
				log.error("record.collectionName must not be null or empty!");
				return false;
			}
			String rollBy = props.getProperty("record.rollBy");
			if("day".equalsIgnoreCase(rollBy) || "month".equalsIgnoreCase(rollBy)) {
				InstanceHolder.rollBy = rollBy;
			} else {
				InstanceHolder.rollBy = "none";
			}
			
			log.info("initializing producer&consumer threads...");
			int cCount = Integer.parseInt(threadCount);
			InstanceHolder.cThreads = new HashSet<ConsumerThread>();
			while(InstanceHolder.cThreads.size() < cCount) {
				ConsumerThread ct = new ConsumerThread();
				InstanceHolder.cThreads.add(ct);
			}
			//启动线程
			for(ConsumerThread ct : InstanceHolder.cThreads) {
				ct.start();
			}
			pt.start();
			//激活定时任务，每5分钟检查线程与队列状况并告警
			timer.schedule(new DaemonTask(pt), 10000, 5*60*1000);
			return true;
		} catch(Throwable t) {
			log.error("initializing breeze-consumer failed", t);
			log.error("failed to start breeze-consumer, cleaning...");
			pt.sigStop();
			for(ConsumerThread ct : InstanceHolder.cThreads) {
				ct.sigStop();
			}
			timer.cancel();
			return false;
		}
	}
	
	public static void main(String[] args) {
		PropertyConfigurator.configure(ConsumerRunner.class.getClassLoader().getResource("log4j.properties"));
		log.info("initializing kafka client...");
		if(ConsumerRunner.initKafkaConsumer()) {
			log.info("initializing mongo client...");
			if(ConsumerRunner.initMongo()) {
				log.info("initializing breeze-consumer...");
				if(ConsumerRunner.initBreezeConsumer()) {
					log.info("breeze-consumer started");
					return;
				}
			}
		}
		log.error("failed to start breeze-consumer");
	}
}
