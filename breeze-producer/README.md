# breeze-producer
用于日志生产者端的jar，提供了便捷的api来初始化Kafka Producer Client，并完成结构化日志对象（POJO）的序列化和异步推送

## 依赖
#### breeze-producer的编译依赖下列jar：
* kafka-clients-0.9.0.1
* lz4-1.2.0
* slf4j-api-1.7.6
* snappy-java-1.1.1.7
* commons-logging-1.2 (版本号非严格要求)
* fastjson-1.2.7 (版本号非严格要求)
* slf4j-simple-1.7.6 (非必须，用于调试时输出Kafka日志到控制台)

#### breeze-producer的运行依赖下列jar：
* kafka-clients-0.9.0.1
* lz4-1.2.0
* slf4j-api-1.7.6
* snappy-java-1.1.1.7
* commons-logging-1.2 (版本号非严格要求)
* fastjson-1.2.7 (版本号非严格要求)
* log4j-1.2.17 (非必须，用于输出日志，可用其他commons-logging支持的日志框架替代)
* slf4j-log4j12-1.7.12 (非必须，用于利用log4j输出Kafka日志)


## 如何使用
#### 引入breeze-producer.jar及其依赖包

如使用maven管理依赖：

    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>0.9.0.1</version>
    </dependency>
    
    <dependency>
        <groupId>com.alibaba</groupId>
        <artifactId>fastjson</artifactId>
        <version>1.2.7</version>
    </dependency>
    
    <dependency>
        <groupId>commons-logging</groupId>
        <artifactId>commons-logging</artifactId>
        <version>1.2</version>
    </dependency>

#### 创建breeze-kafka.properties
breeze-producer的运行依赖于kafka client，需要在classpath下添加breeze-kafka.properties文件
以下是一个基本的breeze-kafka.properties文件样例

    bootstrap.servers=10.0.100.1:9092,10.0.100.2:9092
    acks=1
    retries=3
    batch.size=16384
    key.serializer=org.apache.kafka.common.serialization.StringSerializer
    value.serializer=org.apache.kafka.common.serialization.StringSerializer
    buffer.memory=33554432
    request.timeout.ms=30000
    max.request.size=1048576

更加详细的配置项，请参考[Kafka Document](http://kafka.apache.org/documentation.html#producerconfigs)

#### 初始化
如果breeze-producer尚未初始化，在调用BreezeProducer类的send方法时会自动进行初始化

如果在JSP中使用，可以实现一个ServletContextListener，在容器启动时完成breeze-producer的初始化，下面是一个简单的例子：

    public class BreezeProducerInitListener implements ServletContextListener {
    	private static final Log log = LogFactory.getLog(BreezeProducerInitListener.class);
    
    	public void contextInitialized(ServletContextEvent arg0) {
    		log.info("Initializing KafkaProducerClient...");
    		BreezeProducer.init();
    		log.info("KafkaProducerClient initialization complete");
    	}
    
    	public void contextDestroyed(ServletContextEvent arg0) {
		
    	}

    }


这样breeze-producer就会在JSP容器启动时自动初始化

### 推送结构化日志
BreezeProducer类提供了两个方法可以推送结构化信息

* public void send(String topic, String key, Object o, String collection, Date createTime, Callback callback)

Parameters:
* topic 记录所属的topic
* key 记录的key，用于集群broker的负载均衡，如果目标是Kafka单点服务，此参数请传null
* o 记录对象，必须是POJO
* collection 此记录对应MongoDB中的collection名
* createTime 此记录的产生时间（精确到毫秒）
* callback 推送完成后的回调逻辑

从高可用性角度不建议key传null

使用样例：

    BreezeProducer bp = new BreezeProducer();
    bp.send("breezeTest", o.getId(), o, "testRecords", o.getCreateDate(), new Callback() {
        public void onCompletion(RecordMetadata metadata, Exception e) {
            if (e != null)
                e.printStackTrace();
            System.out.println("message send to partition " + metadata.partition() + ", offset: " + metadata.offset());
        }
    });
