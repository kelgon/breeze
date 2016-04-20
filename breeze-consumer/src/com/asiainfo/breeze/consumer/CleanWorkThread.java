package com.asiainfo.breeze.consumer;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.asiainfo.breeze.util.InstanceHolder;

/**
 * A thread to execute when process exits. Stopping threads gracefully to prevent undone task.
 * @author kelgon
 *
 */
public class CleanWorkThread extends Thread {
	private static final Logger log = Logger.getLogger(CleanWorkThread.class);
	
	public void run() {
		PropertyConfigurator.configure(CleanWorkThread.class.getClassLoader().getResource("log4j.properties"));
		try {
			log.info("stopping DaemonTask");
			InstanceHolder.timer.cancel();
			log.info("stopping producer thread...");
			InstanceHolder.pt.sigStop();
			//examine producer thread state
			while(true) {
				Thread.sleep(100);
				if(State.TERMINATED.equals(InstanceHolder.pt.getState()))
					break;
			}
			//wait until there is no remaining tasks in the blocking queue
			log.info("checking remaining tasks...");
			int count = 0;
			while(true) {
				Thread.sleep(500);
				if(InstanceHolder.queue.size() == 0)
					count++;
				if(count > 4)
					break;
			}
			log.info("stopping consumer threads...");
			for(ConsumerThread ct : InstanceHolder.cThreads) {
				ct.sigStop();
			}
			//wait until all consumer threads finish their task
			while(true) {
				Thread.sleep(500);
				boolean all = true;
				for(ConsumerThread ct : InstanceHolder.cThreads) {
					if(!State.TERMINATED.equals(ct.getState()))
						all = false;
				}
				if(all)
					break;
			}
			log.info("shutting down complete");
		} catch(Throwable t) {
			log.error("error occured while shutting down breeze-consumer!", t);
		}
	}
}
