# breeze
基于Kafka和MongoDB的结构化日志异步记录和可配置化预警框架

## 简介
breeze是一个基于Java开发的结构化日志异步记录和预警框架。实际上你可以使用它来实现任何结构化的（或理解为POJO的）数据，但breeze提供的预警能力仍然最适用于记录日志（特别是具有业务含义的日志）的场景。

breeze使用Kafka进行消息的异步处理。可以让日志产生端与数据的落地端解耦，特别适合有大量业务应用实例在同时产生一类日志的场景。使用Kafka能够让日志的推送在几个CPU tick内完成，几乎完全不影响日志生产者的处理效率。

在日志数据的消费端，breeze使用MongoDB进行日志数据的落地，之所以选择MongoDB，而不是其他关系型数据库或key-value型数据库，是为了兼顾结构化日志的分析能力（MongoDB提供了强大的条件查询能力）和性能（抛弃强一致性事务令MongoDB有着比关系型数据库高得多的性能）

## 组件
* breeze-producer
 是用于日志生产者端的jar，它提供了便捷的api来初始化Kafka Producer Client，并完成结构化日志对象（POJO）的序列化和异步推送

* breeze-consumer
 是独立运行的Java进程，它从Kafka集群中订阅（拉取）待消费的日志信息，将其反序列化并插入MongoDB，支持MongoDB collection的自动滚动切分。breeze-consumer内部使用阻塞队列实现了高效的生产者-消费者模型，可以配置消费者线程的数量，同时支持部署多个实例，能够保证日志消息的及时入库。它使用配置文件进行Kafka Consumer Client、MongoClient以及多线程模型的初始化，支持部分配置的热更新，以及阻塞队列的积压预警等。

* breeze-watcher
 是独立运行的Java进程，基于Quartz scheduler实现。它运行着多个定时任务：
 1. 监控Kafka集群和MongoDB集群的状态，并发出预警
 2. 在MongoDB Collection需要滚动切分前创建新的collection，并可通过配置实现在指定属性上自动创建索引
 3. 对结构化日志进行监控
