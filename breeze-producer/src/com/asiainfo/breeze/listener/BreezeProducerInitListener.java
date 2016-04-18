package com.asiainfo.breeze.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.asiainfo.breeze.producer.BreezeProducer;

public class BreezeProducerInitListener implements ServletContextListener {
	private static final Log log = LogFactory.getLog(BreezeProducerInitListener.class);

	public void contextDestroyed(ServletContextEvent arg0) {
		
	}

	public void contextInitialized(ServletContextEvent arg0) {
		log.info("Initializing KafkaProducerClient...");
		BreezeProducer.init();
		log.info("KafkaProducerClient initialization complete");
	}

}
