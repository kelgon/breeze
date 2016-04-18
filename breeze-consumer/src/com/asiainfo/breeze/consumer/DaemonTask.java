package com.asiainfo.breeze.consumer;

import java.io.InputStream;
import java.lang.Thread.State;
import java.util.Properties;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.asiainfo.breeze.util.AlarmSender;
import com.asiainfo.breeze.util.InstanceHolder;

/**
 * A task to reload and renew breeze-consumer.properties. 
 * Also examine ProducerThread and ConsumerThread.
 * @author kelgon
 *
 */
public class DaemonTask extends TimerTask {
	private final static Logger log = Logger.getLogger(DaemonTask.class);
	private ProducerThread pt;
	public DaemonTask(ProducerThread pt) {
		super();
		this.pt = pt;
	}

	public void run() {
		try {
			log.info("DaemonTask started");
			InputStream is = DaemonTask.class.getClassLoader().getResourceAsStream("breeze-consumer.properties");
			Properties props = new Properties();
			props.load(is);
			//检查ProducerThread状态
			try {
				if(!State.NEW.equals(pt.getState()) && !State.TERMINATED.equals(pt.getState())) {
					log.info("ProducerThread is working, state: " + pt.getState().toString());
				} else {
					log.error("ProducerThread is not working, state: " + pt.getState().toString());
					AlarmSender.sendAlarm("[breeze-consumer] ProducerThread abnormal state: " + pt.getState().toString());
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
						ConsumerThread consumerT = new ConsumerThread();
						InstanceHolder.cThreads.add(consumerT);
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
				AlarmSender.sendAlarm("[breeze-consumer] ConsumerThread state-check failed!");
			}
			//检查队列积压情况
			try {
				int threshold = Integer.parseInt(props.getProperty("record.queueSizeAlarmThreshold"));
				int queueSize = InstanceHolder.queue.size();
				if(queueSize >= threshold) {
					AlarmSender.sendAlarm("[breeze-consumer] queue size exceeded threshold! size: " + queueSize + ", threshold: " + threshold);
				}
			} catch(Throwable t) {
				log.error("ConsumerThread queueSize-check failed", t);
				AlarmSender.sendAlarm("[breeze-consumer] ConsumerThread queueSize-check failed!");
			}
		} catch(Throwable t) {
			log.error("error occured in DaemonTask", t);
			AlarmSender.sendAlarm("[breeze-consumer] error occured in DaemonTask, please check in time");
		}
	}

}
