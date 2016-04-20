package com.asiainfo.breeze.util;

import org.apache.log4j.Logger;

public class AlarmSender {
	private static final Logger log = Logger.getLogger(AlarmSender.class);
	public static void sendAlarm(String alarm) {
		log.warn(alarm);
	}
}
