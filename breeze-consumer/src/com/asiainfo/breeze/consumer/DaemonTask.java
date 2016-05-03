package com.asiainfo.breeze.consumer;

import java.io.InputStream;
import java.lang.Thread.State;
import java.util.Properties;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.asiainfo.breeze.util.AlarmSender;
import com.asiainfo.breeze.util.InstanceHolder;

/**
 * 定时检查producer&consumer线程状态，检查阻塞队列长度并告警，更新consumer线程数量，
 * 更新collection配置
 * @author kelgon
 *
 */
public class DaemonTask extends TimerTask {
	private final static Logger log = Logger.getLogger(DaemonTask.class);
	public DaemonTask() {
		super();
	}

	public void run() {
		try {
			log.info("DaemonTask started");
			InputStream is = DaemonTask.class.getClassLoader().getResourceAsStream("breeze-consumer.properties");
			Properties props = new Properties();
			props.load(is);
			//检查ProducerThread状态
			try {
				if(!State.NEW.equals(InstanceHolder.pt.getState()) && !State.TERMINATED.equals(InstanceHolder.pt.getState())) {
					log.info("ProducerThread is working, state: " + InstanceHolder.pt.getState().toString());
				} else {
					log.error("ProducerThread is not working, state: " + InstanceHolder.pt.getState().toString());
					AlarmSender.sendAlarm("[breeze-consumer] ProducerThread abnormal state: " + InstanceHolder.pt.getState().toString());
				}
			} catch(Throwable t) {
				log.error("ProducerThread state-check failed", t);
				AlarmSender.sendAlarm("[breeze-consumer] ProducerThread state-check failed!");
			}
			//检查ConsumerThread状态，并更新ConsumerThread线程数配置
			try {
				int newCount = Integer.parseInt(props.getProperty("consumer.threadCount"));
				int deadCount = 0;
				int liveCount = 0;
				for(ConsumerThread ct : InstanceHolder.cThreads) {
					if(State.TERMINATED.equals(ct.getState())) {
						deadCount++;
					} else {
						liveCount++;
					}
				}
				log.info("living ConsumerThreads: " + liveCount);
				log.info("dead ConsumerThreads: " + deadCount);
				//补充启动线程
				if(newCount > liveCount) {
					log.info("Starting new ConsumerThread to match consumer.threadCount [" + newCount + "] ...");
					for(int i = 0; i < newCount - liveCount; i++) {
						InstanceHolder.consumerNameCount++;
						ConsumerThread consumerT = new ConsumerThread();
						InstanceHolder.cThreads.add(consumerT);
						consumerT.setName("Consumer-" + InstanceHolder.consumerNameCount);
						consumerT.start();
					}
				}
				//停止多余线程
				if(newCount < liveCount) {
					log.info("Stoping ConsumerThread to match consumer.threadCount [" + newCount + "] ...");
					int i = 0;
					for(ConsumerThread ct : InstanceHolder.cThreads) {
						ct.sigStop();
						i++;
						if(i == liveCount - newCount)
							break;
					}
				}
			} catch(Throwable t) {
				log.error("ConsumerThread state-check failed", t);
				AlarmSender.sendAlarm("breeze-consumer线程状态检查出错："+t.toString());
			}
			
			//检查队列积压情况
			log.info("checking untended tasks in blockingQueue...");
			try {
				int threshold = Integer.parseInt(props.getProperty("record.queueSizeAlarmThreshold"));
				int queueSize = InstanceHolder.queue.size();
				if(queueSize >= threshold) {
					AlarmSender.sendAlarm("breeze-consumer队列积压任务数超阈值，当前队列长度：" + queueSize + "，阈值：" + threshold);
				}
			} catch(Throwable t) {
				log.error("ConsumerThread queueSize-check failed", t);
				AlarmSender.sendAlarm("breeze-consumer阻塞队列检查出错："+t.toString());
			}
			
			//刷新配置
			log.info("reload and renew configurations...");
			String collections = props.getProperty("record.collections");
			if("".equals(collections) || collections == null) {
				log.error("record.collections must not be null or empty!");
				return;
			}
			for(String collection : collections.split(",")) {
				String[] c = collection.split(":");
				InstanceHolder.collectionMap.put(c[0], c[1]);
			}
			String defaultCollection = props.getProperty("record.defaultCollection");
			if("".equals(defaultCollection) || defaultCollection == null) {
				log.error("record.defaultCollection must not be null or empty!");
				return;
			}
			InstanceHolder.defaultCollection = defaultCollection.split(":")[0];
			InstanceHolder.defaultRollBy = defaultCollection.split(":")[1];
			log.info("DaemonTask done.");
		} catch(Throwable t) {
			log.error("error occured in DaemonTask", t);
			AlarmSender.sendAlarm("[breeze-consumer] DaemonTask执行出错"+t.toString());
		}
	}

}
