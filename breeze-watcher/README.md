# breeze-watcher
 独立运行的Java进程，基于Quartz scheduler实现。它运行着多个定时任务：
 1. KafkaClusterHealthCheckJob：监控Kafka topic状态，当topic出现out-of-sync的replica时和无可用Leader时进行告警
 2. MongoReplicaHealthCheckJob：监控MongoDB集群状态，当server状态异常、无Primary节点可用或Primary节点发生切换时进行告警
 3. ZookeeperHealthCheckJob：监控Zookeeper集群状态，当集群server状态异常或无可用Leader时进行告警
 4. CollectionCreateJob：如果breeze-consumer中配置了collection按时间分片，此任务会创建新的collection机器索引，同时进行过期collection的删除
 5. DefinedMonitorJob：用户自定义的监控任务
 
## 依赖
#### breeze-watcher的编译和运行依赖下列jar：
* mongo-java-driver-3.2.*
* kafka-clients-0.9.0.1
* lz4-1.2.0
* slf4j-api-1.7.6
* snappy-java-1.1.1.7
* log4j-1.2.17 (版本号非严格要求)
* slf4j-log4j12-1.7.12 (版本号非严格要求)
* quartz-2.2.*
* quartz-jobs-2.2.*
* parsii-2.3
* commons-net-3.4

release目录下的zip包已经包含了所有需要的jar包

## 准备工作
#### 修改配置文件
breeze-consumer.jar中共有4个properties文件，在启动前先需要根据实际环境进行修改

##### breeze-kafka.properties

KafkaConsumer的相关初始化配置，如无特殊需求，只需修改bootstrap.servers为实际使用的kafka集群地址即可

其他具体的配置项，请参考[Kafka Document](http://kafka.apache.org/documentation.html#newconsumerconfigs)

##### breeze-mongo.properties

MongoDB的相关配置，需要关注的配置包括：
* mongo.servers: MongoDB集群服务器列表
* breeze.adminDbName: 用于监控集群状态的db名称
* breeze.adminCredentials: 用于监控集群状态的db的credential，格式为user:password，如无需身份验证，可配置为空
* breeze.monitorDbName: 用于存储自定义监控任务的db名称
* breeze.monitorCredentials: 略
* breeze.recordDbName: 执行collection切分和过期collection删除的目标db名称
* breeze.recordCredentials: 略

其他具体的配置项，请参考[MongoClientOptions](http://api.mongodb.org/java/3.2/com/mongodb/MongoClientOptions.html)

##### breeze-watcher.properties

breeze-watcher本身的相关配置

    #各任务的执行计划
    KafkaClusterHealthCheckJob.cron=0 0/5 * * * ?
    MongoReplicaHealthCheckJob.cron=0 0/5 * * * ?
    ZookeeperHealthCheckJob.cron=0 0/5 * * * ?
    DefinedMonitorJob.cron=0 0/1 * * * ?
    CollectionCreateJob.cron=0 30 23 * * ?
    #监控的Kafka topic名，逗号分隔
    consumer.kafkaTopics=breezeTest,crashTest
    #Kafka topic对应的副本数量
    breezeTest.replicaCount=2
    crashTest.replicaCount=1
    #zookeeper服务列表，逗号分隔
    zookeeper.nodes=
    #存储记录的collection名，逗号分隔
    breeze.recordCollections=coll1,coll2
    #每个collection的切分方式
    coll1.rollBy=day
    #每个collection上需创建的索引，如要创建多个索引，逗号分隔
    coll1.index=date
    #每个collection的保留时间，如果rollBy为day，则单位为天，如果rollBy为month，则单位为月
    coll1.deleteAfter=60
    coll2.rollBy=day
    coll2.index=date
    coll2.deleteAfter=60

#### 编写自定义监控策略

DefinedMonitorJob会从breeze.monitorDbName的definedMonitor集合中获取自定义监控列表，并定时运行。

合法的自定义监控格式如下：

    {
    	jobName: 监控名称
    	online: 配置成yes以外的任何值，DefinedMonitorJob都会跳过此监控不执行
    	lastRun: 此监控上次运行的时间，格式为yyyy-MM-dd HH:mm"，初次运行时，将此属性设置为一个较早的时间即可
    	interval: 运行间隔，单位为分钟
    	alarm: 告警内容的文字模板，动态项包括：当前值{currentValue}，阈值{threshold}，历史值{historyValue}，变化幅度{percent}，报告值{报告名称}
    	alarmCondition: 触发告警的条件对象，如果此属性的值不是一个合法的Bson Document，则DefinedMonitorJob会跳过对其的解析，默认为强制触发告警。也就是说，如果你打算定义一个报告类型的监控（即定时产生报告，无论值是多少都触发告警的发送），可以简单地将此属性定义为""
    	{
    		collection: 查询的目标集合，格式为[集合名:切分方式]，如"coll1:day"
    		opType: aggregate或find，DefinedMonitorJob会执行指定的操作
    		operator: currentValue与threshold的比较方法，包括">","<","=","!=",">="和"<="
    		threshold: 阈值，应配置为数字
    		expression: 执行aggregate或find命令时传入的Bson Document，注意双引号的转义。如果命令的条件包含当前时间，则用{sysdate}代替，例如要查询创建时间(createDate)在5分钟内的记录，则{createDate:{$gt:"{sysdate - 5*60*1000}"}}。此操作返回的结果中必须包含result字段，DefinedMonitorJob会取该字段的值为结果值。
    		compareToHistory: 如果希望定义一个基于变化趋势的告警，可将此属性配置为inc或dec，即增长率和下降率。例如此值配置为inc时，DefinedMonitorJob会将currentValue与此监控在一天前和一周前同一时间点的执行结果进行比较，如果增长率高于threshold则触发告警。如果不希望与历史记录对比，请将此值配置为""
    	},
    	reports: 报告对象，有些时候可能会希望在告警文字内容中附加一些额外的信息，这类信息可用report进行配置。只要满足了alarmCondition中告警触发的条件，DefinedMonitorJob就会按reports的配置获取报告值，并加入告警文字中。支持多个report，所以reports属性必须配置为数组。如果不希望配置报告，可将reports属性设为""
    	[
    		{
    			collection: 查询的目标集合，格式为[集合名:切分方式]，如"coll1:day"
    			reportName: 报告名称，应于告警内容文字幕版中的报告值匹配，如"{rep1}"
    			expression: 同alarmCondition.expression，区别在于reports只支持aggregate操作
    		},
    		{
    			collection: 同上
    			reportName: 同上
    			expression: 同上
    		},
    		...
    	],
    	receivers: 将告警发送给指定的人，可配置成手机号或邮箱地址等等，逗号分隔
    }

样例：

每5分钟执行一次，查询records集合中createDate在5分钟内，且state为failed的记录数量，数量大于等于5时进行告警。records集合为一个按天切分的集合

    {
    	jobName: "自定义监控1",
    	online: "yes",
    	lastRun: "2016-05-04 12:00",
    	interval: 5,
    	alarm: "5分钟内failed记录告警，当前值{currentValue}，阈值{threshold}",
    	alarmCondition: 
    	{
    		collection: "records:day",
    		opType: "aggregate",
    		operator: ">=",
    		threshold: 5,
    		expression: "[{$match: {createDate: {$gt: \"{sysdate-5*60*1000}\"}, state: \"failed\"},{$group: {_id: null,result: {$sum: 1}}}]",
    		compareToHistory: ""
    	},
    	reports: "",
    	receivers: "admin,user1,user2"
    }


每5分钟执行一次，查询records集合中createDate在10分钟内，且state为timeout的记录数量，数量大于等于5时进行告警，同时报告10分钟内costTime字段的平均值。records为一个按月切分的集合：

    {
    	jobName: "自定义监控2",
    	online: "yes",
    	lastRun: "2016-05-04 12:00",
    	interval: 10,
    	alarm: "10分钟内超时数告警，当前值{currentValue}，阈值{threshold}，10分钟内costTime平均值{avgCost}",
    	alarmCondition: 
    	{
    		collection: "records:month",
    		opType: "aggregate",
    		operator: ">=",
    		threshold: 5,
    		expression: "[{$match: {createDate: {$gt: \"{sysdate-10*60*1000}\"}, state: \"timeout\"},{$group: {_id: null,result: {$sum: 1}}}]",
    		compareToHistory: ""
    	},
    	reports: 
    	[
    		{
    			collection: "records:month",
    			reportName: "{avgCost}",
    			expression: "[{$match: {createDate: {$gt: \"{sysdate-10*60*1000}\"}, state: \"timeout\"},{$group: {_id: null,result: {$avg: \"$costTime\"}}}]"
    		}
    	],
    	receivers: "admin,user1,user2"
    }


每小时执行一次，报告records集合中createDate在1小时内的记录总数和state为success的记录总数，record为一个未切分的集合：

    {
    	jobName: "自定义监控3",
    	online: "yes",
    	lastRun: "2016-05-04 12:00",
    	interval: 60,
    	alarm: "1小时内records新纪录数{countAll}，success记录数{countSuccess}",
    	alarmCondition: "",
    	reports: 
    	[
    		{
    			collection: "records",
    			reportName: "{countAll}",
    			expression: "[{$match: {createDate: {$gt: \"{sysdate-3600*1000}\"}},{$group: {_id: null,result: {$sum: 1}}}]"
    		},
    		{
    			collection: "records",
    			reportName: "{countSuccess}",
    			expression: "[{$match: {createDate: {$gt: \"{sysdate-3600*1000}\"}, state: \"success\"},{$group: {_id: null,result: {$sum: 1}}}]"
    		}
    	],
    	receivers: "admin,user1,user2"
    }


每小时执行一次，将records集合中createDate在1小时内的记录总数与历史数据比较，增长率超过100%时告警，record为一个按天切分的集合：

    {
    	jobName: "自定义监控4",
    	online: "yes",
    	lastRun: "2016-05-04 12:00",
    	interval: 5,
    	alarm: "records新记录数增长率告警，1小时内新记录数{currentValue}，历史同期值{historyValue}，增长率{percent}%，阈值{threshold}%",
    	alarmCondition: 
    	{
    		collection: "records:day",
    		opType: "aggregate",
    		operator: ">=",
    		threshold: 100,
    		expression: "[{$match: {createDate: {$gt: \"{sysdate-5*60*1000}\"}},{$group: {_id: null,result: {$sum: 1}}}]",
    		compareToHistory: "inc"
    	},
    	reports: "",
    	receivers: "admin,user1,user2"
    }