package com.asiainfo.breeze.watcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.net.telnet.TelnetClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import com.asiainfo.breeze.util.AlarmSender;
import com.asiainfo.breeze.util.InstanceHolder;

/**
 * Zookeeper集群健康监测任务
 * @author kelgon
 *
 */
public class ZookeeperHealthCheckJob implements Job {
	private final static Logger log = Logger.getLogger(ZookeeperHealthCheckJob.class);
	public void execute(JobExecutionContext context) {
		try {
			PropertyConfigurator.configure(ZookeeperHealthCheckJob.class.getClassLoader().getResource("log4j.properties"));
			log.info("loading breeze-watcher.properties...");
			InputStream is = ZookeeperHealthCheckJob.class.getClassLoader().getResourceAsStream("breeze-watcher.properties");
			Properties props = new Properties();
			props.load(is);
			//获取待检查的zookeeper节点
			String zkNodes = props.getProperty("zookeeper.nodes");
			if("".equals(zkNodes) || zkNodes == null) {
				log.error("zookeeper.nodes must not be null or empty!");
				AlarmSender.sendAlarm("[breeze-watcher] zookeeper监控任务异常: zookeeper.nodes未配置");
				return;
			}
			for(String zk : zkNodes.split(",")) {
				InstanceHolder.zkNodes.put(zk, "failed to connect");
			}
			//依次获取所有zookeeper节点状态
			for(Entry<String, String> e : InstanceHolder.zkNodes.entrySet()) {
				try {
					//通过telnet连接并发送srvr命令
					TelnetClient client = new TelnetClient();
					client.setConnectTimeout(5000);
					client.connect(e.getKey().split(":")[0], Integer.parseInt(e.getKey().split(":")[1]));
					PrintStream out = new PrintStream(client.getOutputStream());
					InputStreamReader isr = new InputStreamReader(client.getInputStream());  
					BufferedReader in = new BufferedReader(isr);  
					out.print("srvr");
					out.flush();
		            String str = null;
		            while ((str = in.readLine()) != null) {
		            	//获取Mode字段，并更新Map中的值
		            	if(str.startsWith("Mode:")) {
		            		String state = str.split(":")[1].trim();
		            		e.setValue(state);
		            	}
		            }
		            client.disconnect();
				} catch(IOException ie) {
					log.error("Telnet连接zookeeper异常", ie);
				}
			}
			String leader = null;
			//遍历Map，寻找状态异常的节点
			for(Entry<String, String> e : InstanceHolder.zkNodes.entrySet()) {
				log.info("zookeeper节点 [" + e.getKey() + "] 状态：" + e.getValue());
				//状态既不是leader也不是follower的节点，告警
				if(!"follower".equals(e.getValue()) && !"leader".equals(e.getValue())) {
					log.warn("zookeeper节点 [" + e.getKey() + "] 状态异常：" + e.getValue());
					AlarmSender.sendAlarm("zookeeper节点 [" + e.getKey() + "] 状态异常：" + e.getValue());
				}
				if("leader".equals(e.getValue()))
					leader = e.getKey();
			}
			//如果不存在Leader，告警
			if(leader == null) {
				AlarmSender.sendAlarm("[严重告警] Zookeeper集群中当前没有可用Leader！");
			}
		} catch(Throwable t) {
			log.error("error occured in ZookeeperHealthCheckJob", t);
			AlarmSender.sendAlarm("[breeze] ZookeeperHealthCheckJob 运行异常：" + t.toString());
		}
	}
	
}
