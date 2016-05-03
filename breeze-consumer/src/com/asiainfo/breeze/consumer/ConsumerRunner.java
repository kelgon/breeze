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

/**
 * breeze-consumer启动线程
 * @author kelgon
 *
 */
public class ConsumerRunner {
	private static final Logger log = Logger.getLogger(ConsumerRunner.class);
	
	/**
	 * 初始化KafkaConsumer
	 * @return
	 */
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
	
	/**
	 * 初始化MongoDB client
	 * @return
	 */
	private static boolean initMongo() {
		try {
			log.info("loading breeze-mongo.properties...");
			InputStream is = ConsumerRunner.class.getClassLoader().getResourceAsStream("breeze-mongo.properties");
			Properties props = new Properties();
			props.load(is);
			
			//构造MongoDB集群serverList
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
			
			//构造MongoDB身份认证对象
			String recordDbName = props.getProperty("breeze.recordDbname");
			if("".equals(recordDbName) || recordDbName == null) {
				log.error("record.dbname must not be null or empty!");
				return false;
			}
			String recordCredentials = props.getProperty("breeze.recordCredentials");
			List<MongoCredential> mCreList = new ArrayList<MongoCredential>();
			if(!"".equals(recordCredentials) && recordCredentials != null) {
				String[] cre = recordCredentials.split(":");
				MongoCredential credential = MongoCredential.createScramSha1Credential(cre[0], recordDbName, cre[1].toCharArray());
				mCreList.add(credential);
			}
			
			//从配置文件加载MongoDB客户端参数
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
			
			//initialize mongodb client
			log.info("initializing mongodb client...");
			if(mCreList.size() > 0)
				InstanceHolder.mClient = new MongoClient(serverList, mCreList, options.build());
			else
				InstanceHolder.mClient = new MongoClient(serverList);
			InstanceHolder.recordMdb = InstanceHolder.mClient.getDatabase(recordDbName);
			return true;
		} catch(Throwable t) {
			log.error("initializing MongoClient failed", t);
			return false;
		}
	}
	
	/**
	 * 初始化breeze-consumer
	 * @return
	 */
	private static boolean initBreezeConsumer() {
		InstanceHolder.pt = new ProducerThread();
		InstanceHolder.timer = new Timer();
		try {
			log.debug("loading breeze-consumer.properties...");
			InputStream is = ConsumerRunner.class.getClassLoader().getResourceAsStream("breeze-consumer.properties");
			Properties props = new Properties();
			props.load(is);
			
			//初始化阻塞队列
			log.info("initializing blocking queue...");
			String queueSize = props.getProperty("consumer.queueSize");
			if("".equals(queueSize) || queueSize == null) {
				log.error("consumer.queueSize must not be null or empty!");
				return false;
			}
			if("0".equals(queueSize))
				InstanceHolder.queue = new LinkedBlockingQueue<String>();
			else
				InstanceHolder.queue = new LinkedBlockingQueue<String>(Integer.parseInt(queueSize));
			
			//订阅Kafka topic
			log.info("subscribing topic...");
			String topic = props.getProperty("consumer.kafkaTopic");
			if("".equals(topic) || topic == null) {
				log.error("consumer.kafkaTopic must not be null or empty!");
				return false;
			}
			InstanceHolder.kc.subscribe(Arrays.asList(topic));
			
			//加载并生成Collection name
			String collections = props.getProperty("record.collections");
			if("".equals(collections) || collections == null) {
				log.error("record.collections must not be null or empty!");
				return false;
			}
			for(String collection : collections.split(",")) {
				String[] c = collection.split(":");
				InstanceHolder.collectionMap.put(c[0], c[1]);
			}
			String defaultCollection = props.getProperty("record.defaultCollection");
			if("".equals(defaultCollection) || defaultCollection == null) {
				log.error("record.defaultCollection must not be null or empty!");
				return false;
			}
			InstanceHolder.defaultCollection = defaultCollection.split(":")[0];
			InstanceHolder.defaultRollBy = defaultCollection.split(":")[1];
			
			//初始化producer&consumer线程
			log.info("initializing producer&consumer threads...");
			String threadCount = props.getProperty("consumer.threadCount");
			if("".equals(threadCount) || threadCount == null) {
				log.error("consumer.threadCount must not be null or empty!");
				return false;
			}
			int cCount = Integer.parseInt(threadCount);
			InstanceHolder.cThreads = new HashSet<ConsumerThread>();
			int i=0;
			while(InstanceHolder.cThreads.size() < cCount) {
				ConsumerThread ct = new ConsumerThread();
				ct.setName("Consumer-"+i);
				InstanceHolder.cThreads.add(ct);
				i++;
			}
			InstanceHolder.consumerNameCount = i;
			
			//启动consumer&producer线程
			log.info("launching threads...");
			for(ConsumerThread ct : InstanceHolder.cThreads) {
				ct.start();
			}
			InstanceHolder.pt.start();
			
			//注册dameon线程，每5分钟执行一次
			InstanceHolder.timer.schedule(new DaemonTask(), 10000, 5*60*1000);
			
			return true;
		} catch(Throwable t) {
			log.error("initializing breeze-consumer failed", t);
			//explicitly exit to trigger cleaning mechanism
			System.exit(0);
			return false;
		}
	}
	
	public static void main(String[] args) {
		PropertyConfigurator.configure(ConsumerRunner.class.getClassLoader().getResource("log4j.properties"));
		//注册进程退出hook，在进程退出时执行清理逻辑
		Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
		log.info("initializing mongo client...");
		if(ConsumerRunner.initMongo()) {
			log.info("initializing kafka client...");
			if(ConsumerRunner.initKafkaConsumer()) {
				log.info("initializing breeze-consumer...");
				if(ConsumerRunner.initBreezeConsumer()) {
					log.info("breeze-consumer started");
					return;
				}
			}
		}
		log.error("failed to start breeze-consumer");
		System.exit(0);
	}
}
