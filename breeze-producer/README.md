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

#### breeze-producer的使用依赖下列jar：
* kafka-clients-0.9.0.1
* lz4-1.2.0
* slf4j-api-1.7.6
* snappy-java-1.1.1.7
* commons-logging-1.2 (版本号非严格要求)
* fastjson-1.2.7 (版本号非严格要求)
* log4j-1.2.17 (非必须，用于输出日志，可用其他commons-logging支持的日志框架替代)
* slf4j-log4j12-1.7.12 (非必须，用于利用log4j输出Kafka日志)

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




