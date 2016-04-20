# breeze-consumer
独立运行的Java进程，它从Kafka集群中订阅（拉取）待消费的日志信息，将其反序列化并插入MongoDB，支持MongoDB collection的自动滚动切分。breeze-consumer内部使用阻塞队列实现了高效的生产者-消费者模型，可以配置消费者线程的数量，同时支持部署多个实例，能够保证日志消息的及时入库。它使用配置文件进行Kafka Consumer Client、MongoClient以及多线程模型的初始化，支持部分配置的热更新，以及阻塞队列的积压预警等。

## 依赖
#### breeze-consumer的编译和运行需要1.5以上的JDK，以及下列jar：
* kafka-clients-0.9.0.1
* lz4-1.2.0
* slf4j-api-1.7.6
* snappy-java-1.1.1.7
* mongo-java-driver-3.2.*
* log4j-1.2.17 (版本号非严格要求)
* slf4j-log4j12-1.7.12 (版本号非严格要求)

bin目录下的tar包已经包含了所有依赖的jar包

## 如何使用
#### 修改配置文件
breeze-consumer.jar中共有三个properties文件，在启动前先需要根据实际环境进行修改

##### breeze-kafka.properties

KafkaConsumer的相关初始化配置，如无特殊需求，只需修改bootstrap.servers为实际使用的kafka集群地址即可

其他具体的配置项，请参考[Kafka Document](http://kafka.apache.org/documentation.html#newconsumerconfigs)

##### breeze-mongo.properties

MongoDB的相关配置，需要关注的配置包括：
* mongo.servers: MongoDB集群服务器列表
* mongo.connectionsPerHost: MondoDB连接池最大容量，可参考consumer线程数进行配置
* breeze.recordDbname: 使用的MongoDB database名称
* breeze.recordCredentials: 用于认证该database的用户名和密码，如未启用认证，可以不填

其他具体的配置项，请参考[MongoClientOptions](http://api.mongodb.org/java/3.2/com/mongodb/MongoClientOptions.html)

##### breeze-consumer.properties

breeze-consumer本身的相关配置

    #Kafka topic名
    consumer.kafkaTopic=breezeTest
    #consumer线程数量
    consumer.threadCount=50
    #队列最大长度，0为不限
    consumer.queueSize=0
    #如果kafka中的record没有指定collection，默认插入的mongodb collection名
    record.defaultCollectionName=testRecord
    #record collection的滚动时间片，支持day,month或none
    record.rollBy=day
    #任务积压告警阈值
    record.queueSizeAlarmThreshold=3000


#### 修改AlarmSender（可选）
在DaemonTask线程检查发现breeze-consumer的ProducerThread和ConsumerThread出现问题时，以及发现阻塞队列内积压的任务数量超过阈值时，会通过AlarmSender类进行告警。

默认的告警方式是向一个单独的文件中输出告警日志（alarm.log），如果你希望通过其他方式进行告警（如短信、邮件、http等），可对AlarmSender类进行改造，或者启动一个独立的进程持续检查alarm.log，以实现其他形式的告警。
