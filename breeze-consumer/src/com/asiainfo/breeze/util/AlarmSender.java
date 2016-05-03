package com.asiainfo.breeze.util;

import org.apache.log4j.Logger;

/**
 * 发送告警消息的实现类，可根据具体情况实现sendAlarm方法
 * @author kelgon
 *
 */
public class AlarmSender {
	private static final Logger log = Logger.getLogger(AlarmSender.class);
	public static void sendAlarm(String alarm) {
		log.warn(alarm);
	}
	public static void sendAlarm(String alarm, String[] receivers) {
		log.warn(alarm);
	}
}
