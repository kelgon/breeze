package com.asiainfo.breeze.watcher;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import parsii.eval.Parser;
import parsii.eval.Scope;
import parsii.eval.Variable;
import parsii.tokenizer.ParseException;

import com.asiainfo.breeze.util.AlarmSender;
import com.asiainfo.breeze.util.InstanceHolder;
import com.mongodb.client.FindIterable;
import com.mongodb.util.JSON;

/**
 * 自定义监控任务
 * @author kelgon
 *
 */
public class DefinedMonitorJob implements Job {
	private static final Logger log = Logger.getLogger(DefinedMonitorJob.class);
	public void execute(JobExecutionContext context) {
		log.info("DefinedMonitorJob started");
		try {
			//从monitor库中查询所有online=yes的自定义监控数据
			FindIterable<Document> docs = InstanceHolder.monitorMdb.getCollection("definedMonitor").find(new Document("online","yes"));
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			//遍历监控任务，启动应执行的任务（即 [上次执行时间+执行间隔] 大于当前时间的任务）
			for(Document doc : docs) {
				Date lastRun = sdf.parse(doc.getString("lastRun"));
				double interval = Double.parseDouble(String.valueOf(doc.get("interval")));
				if(lastRun.getTime() + interval*60*1000 < new Date().getTime()) {
					log.info("待执行监控: " + doc.getString("jobName"));
					MonitorJobThread t = new MonitorJobThread(doc);
					t.start();
				}
			}
		} catch(Throwable t) {
			log.error("error occured in DefinedMonitorJob", t);
			AlarmSender.sendAlarm("[breeze] watcher自定义监控任务启动异常" + t.toString());
		}
	}
	
	/**
	 * 自定义监控线程
	 * @author kelgon
	 *
	 */
	class MonitorJobThread extends Thread {
		private Document doc;
		MonitorJobThread(Document doc) {
			super();
			this.doc = doc;
		}

		/**
		 * 将查询JSON中的{sysdate}运算算式替换为具体的时间
		 * @param in 待处理的JSON
		 * @return 处理完成的JSON
		 * @throws ParseException 解析运算算式出错
		 */
		private String parseDate(String in) throws ParseException {
			Set<String> exprs = new HashSet<String>();
			int start = in.indexOf("\"{sysdate");
			while(start != -1) {
				int end = in.indexOf("}\"", start);
				if(end == -1)
					return null;
				String expr = in.substring(start, end+2);
				exprs.add(expr);
				start = in.indexOf("\"{sysdate", end);
			}
			Date date = new Date();
			Scope scope = new Scope();
			for(String e : exprs) {
				String expr = e.substring(2,e.length()-2);
				Variable sysdate = scope.getVariable("sysdate");
				sysdate.setValue(date.getTime());
				long res = (long)Parser.parse(expr, scope).evaluate();
				in = in.replace(e, String.valueOf(res));
			}
			return in;
		}
		
		/**
		 * 将name:timeSlice格式的collection定义转换成具体的collection名。如在2016年4月28日运行此方法，传入"test:day"，则会返回"test_20160428"
		 * @param collectionStr name:timeSlice格式的collection定义
		 * @return 转换后的collection名
		 */
		private String getCollectionName(String collectionStr) {
			String collection = null;
			String[] c = collectionStr.split(":");
			if(c.length == 1) {
				collection = c[0];
			} else if(c.length == 2) {
				if("day".equalsIgnoreCase(c[1])) {
					SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
					collection = c[0] + "_" + sdf1.format(new Date());
				} else if("month".equalsIgnoreCase(c[1])) {
					SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMM");
					collection = c[0] + "_" + sdf1.format(new Date());
				} else {
					collection = c[0];
				}
			}
			return collection;
		}
		
		public void run() {
			try {
				Date d = new Date();
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
				String date = sdf.format(d);
				doc.put("lastRun", date);
				//回写mongodb，将本任务的lastRun置为当前时间，格式yyyy-MM-dd HH:mm
				InstanceHolder.monitorMdb.getCollection("definedMonitor").updateOne(new Document("_id", doc.getObjectId("_id")), new Document("$set", new Document("lastRun", date)));
				
				Object aco = doc.get("alarmCondition");
				String alarm = doc.getString("alarm"); //告警内容模板
				boolean triggerAlarm = false; //标识是否需要触发告警的flag
				
				//如果alarmCondition是Document对象，则解析对象并执行计算，否则跳过告警触发的条件计算，强制触发告警
				if(aco instanceof Document) {
					Document condition = (Document)aco;
					String collection = getCollectionName(condition.getString("collection"));
					//处理expression串，将sysdate替换为当前时间
					String expression = condition.getString("expression");
					expression = parseDate(expression);
					String opType = condition.getString("opType"); //计算类型，find或aggregate
					String operator = condition.getString("operator"); //与阈值的比较符
					if(collection == null) {
						log.error("自定义监控 [" + doc.getString("jobName") + "] alarmCondition.collection配置有误：" + condition.getString("collection"));
						return;
					}
					if(expression == null) {
						log.error("自定义监控 [" + doc.getString("jobName") + "] alarmCondition.expression配置有误");
						return;
					}
					
					Document resDoc; //结果值doc
					if("find".equals(opType)) {
						//执行find操作
						resDoc = InstanceHolder.recordMdb.getCollection(collection).find(Document.parse(expression)).first();
					} else if("aggregate".equals(opType)) {
						//执行aggregate操作
						//将expression串转换为Bson数组
						List<Bson> agList = new ArrayList<Bson>();
						for(Object o : (ArrayList<?>)JSON.parse(expression)) {
							if(o instanceof Bson)
								agList.add((Bson)o);
						}
						resDoc = InstanceHolder.recordMdb.getCollection(collection).aggregate(agList).first();
					} else {
						log.error("自定义监控 [" + doc.getString("jobName") + "] alarmCondition.opType配置有误：" + opType);
						return;
					}

					String compareValue = String.valueOf(condition.get("threshold")); //阈值
					String currentValue;//find或aggregate的结果，如果操作未找到符合要求的数据，则默认为0
					if(resDoc != null)
						currentValue = String.valueOf(resDoc.get("result")); 
					else
						currentValue = "0";
					log.info("自定义监控 [" + doc.getString("jobName") + "] alarmCondition结果值：" + currentValue);
					
					if("".equals(condition.getString("compareToHistory"))) {
						//将结果与阈值比较，符合条件则将triggerAlarm置为true
						if("=".equals(operator)) {
							if(compareValue.equals(currentValue))
								triggerAlarm = true;
						} else if("!=".equals(operator)) {
							if(!compareValue.equals(currentValue))
								triggerAlarm = true;
						} else if(">".equals(operator)) {
							if(Double.parseDouble(currentValue) > Double.parseDouble(compareValue))
								triggerAlarm = true;
						} else if("<".equals(operator)) {
							if(Double.parseDouble(currentValue) < Double.parseDouble(compareValue))
								triggerAlarm = true;
						} else if(">=".equals(operator)) {
							if(Double.parseDouble(currentValue) >= Double.parseDouble(compareValue))
								triggerAlarm = true;
						} else if("<=".equals(operator)) {
							if(Double.parseDouble(currentValue) >= Double.parseDouble(compareValue))
								triggerAlarm = true;
						} else {
							log.error("自定义监控 [" + doc.getString("jobName") + "] alarmCondition.operator配置无效：" + opType);
							return;
						}
						
						//将告警内容模板中的阈值和当前值关键字替换为实际的值
						if(triggerAlarm) {
							alarm = alarm.replace("{threshold}", compareValue).replace("{currentValue}", currentValue);
						}
					} else {
						//将结果与历史值（前一天同一时段和一周前同一天的同一时段）比较，变化幅度大于阈值则告警
						String compareTypes = condition.getString("compareToHistory");
						boolean incComp = false;//与历史值比较增长率
						boolean decComp = false;//与历史之比较下降率
						if("inc".equals(compareTypes))
							incComp = true;
						if("dec".equals(compareTypes))
							decComp = true;
						
						//将本次值写入历史库
						sdf = new SimpleDateFormat("yyyyMMddHHmm");
						InstanceHolder.recordMdb.getCollection("historyRecord").insertOne(new Document("monitorJobId", String.valueOf(doc.getObjectId("_id"))).append("date", sdf.format(d)).append("value", currentValue));

						//取得前一天同一时段和一周前同一天的同一时段的值
						String yesterday = sdf.format(new Date(d.getTime()-24*3600*1000));
						String lastWeek = sdf.format(new Date(d.getTime()-7*24*3600*1000));
						Document yDoc = InstanceHolder.recordMdb.getCollection("historyRecord").find(new Document("monitorJobId", String.valueOf(doc.getObjectId("_id"))).append("date", yesterday)).first();
						Document lwDoc = InstanceHolder.recordMdb.getCollection("historyRecord").find(new Document("monitorJobId", String.valueOf(doc.getObjectId("_id"))).append("date", lastWeek)).first();
						Integer yesterdayValue = null;
						Integer lastWeekValue = null;
						if(yDoc != null && yDoc.get("value") != null) {
							yesterdayValue = Integer.parseInt(String.valueOf(yDoc.get("value")));
						}
						if(lwDoc != null && lwDoc.get("value") != null) {
							lastWeekValue = Integer.parseInt(String.valueOf(lwDoc.get("value")));
						}
						int nowValue = Integer.parseInt(currentValue);//当前值int
						String historyString = "";//用于替换告警语模板的字串
						double percent = 0;//百分比阈值
						if(yesterdayValue != null || lastWeekValue != null) {
							//比较前一天值和前一周值，得出两个值中较小的与较大的
							String largerDay,lesserDay;
							int largerValue,lesserValue;
							if(yesterdayValue != null && lastWeekValue == null) {
								largerDay = yesterday;
								largerValue = yesterdayValue;
								lesserDay = yesterday;
								lesserValue = yesterdayValue;
							} else if(yesterdayValue == null && lastWeekValue != null) {
								largerDay = lastWeek;
								largerValue = lastWeekValue;
								lesserDay = lastWeek;
								lesserValue = lastWeekValue;
							} else {
								if(yesterdayValue > lastWeekValue) {
									largerDay = yesterday;
									largerValue = yesterdayValue;
									lesserDay = lastWeek;
									lesserValue = lastWeekValue;
								} else {
									largerDay = lastWeek;
									largerValue = lastWeekValue;
									lesserDay = yesterday;
									lesserValue = yesterdayValue;
								}
							}
							
							if(incComp) {
								//计算增长率，将当前值与【昨天值和上周值中较小的一个】比较
								if(nowValue > lesserValue) {
									percent = ((double)nowValue/(double)lesserValue - 1)*100;
									if(percent > Integer.parseInt(compareValue)) {
										triggerAlarm = true;
										historyString = lesserValue + "(" + lesserDay + ")";
									}
								}
							}
							if(decComp) {
								//计算下降率，将当前值与【昨天值和上周值中较大的一个】比较
								if(nowValue < largerValue) {
									percent = (1 - (double)nowValue/(double)largerValue)*100;
									if(percent > Integer.parseInt(compareValue)) {
										triggerAlarm = true;
										historyString = largerValue + "(" + largerDay + ")";
									}
								}
							}
						}
						
						//将告警内容模板中的阈值和当前值关键字替换为实际的值
						if(triggerAlarm) {
							BigDecimal bd = new BigDecimal(percent);
							alarm = alarm.replace("{threshold}", compareValue).replace("{currentValue}", currentValue).replace("{historyValue}", historyString).replace("{percent}", bd.setScale(2, BigDecimal.ROUND_HALF_DOWN).toString());
						}
					}
				} else {
					triggerAlarm = true;
				}
				
				//进行report的计算
				Object reportsO = doc.get("reports");
				if(reportsO instanceof List<?>) {
					List<?> reports = (List<?>)reportsO;
					for(Object reportO : reports) {
						Document report = (Document)reportO;
						String collection = getCollectionName(report.getString("collection"));
						String expression = report.getString("expression");
						expression = parseDate(expression);
						List<Bson> agList = new ArrayList<Bson>();
						for(Object o : (ArrayList<?>)JSON.parse(expression)) {
							if(o instanceof Bson)
								agList.add((Bson)o);
						}
						String reportName = report.getString("reportName");
						expression = parseDate(expression);
						Document resDoc = InstanceHolder.recordMdb.getCollection(collection).aggregate(agList).first();
						String result = String.valueOf(resDoc.get("result"));
						alarm = alarm.replace(reportName, result);
						log.info("自定义监控 [" + doc.getString("jobName") + "] report[" + reportName + "] 结果值：" + result);
					}
				}
				
				//发送告警
				if(triggerAlarm) {
					log.warn("发送监控告警：[" + alarm + "] 至 [" + doc.getString("receivers") + "]");
					AlarmSender.sendAlarm(alarm, doc.getString("receivers").split(","));
				} else {
					log.info("[" + doc.getString("jobName") + "] 未触发告警");
				}
			} catch(Throwable t) {
				log.error("自定义监控 [" + doc.getString("jobName") + "] 执行异常", t);
				AlarmSender.sendAlarm("自定义监控 [" + doc.getString("jobName") + "] 执行异常：" + t.toString());
			}
		}
	}
}
