package com.asiainfo.breeze.watcher;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
		
		/**
		 * 判断两个传入值是否满足operator运算符
		 * @param operator
		 * @param compareValue
		 * @param resultValue
		 * @return
		 */
		private Boolean compare(String operator, String compareValue, String resultValue) {
			if("=".equals(operator)) {
				if(compareValue.equals(resultValue))
					return true;
			} else if("!=".equals(operator)) {
				if(!compareValue.equals(resultValue))
					return true;
			} else if(">".equals(operator)) {
				if(Double.parseDouble(resultValue) > Double.parseDouble(compareValue))
					return true;
			} else if("<".equals(operator)) {
				if(Double.parseDouble(resultValue) < Double.parseDouble(compareValue))
					return true;
			} else if(">=".equals(operator)) {
				if(Double.parseDouble(resultValue) >= Double.parseDouble(compareValue))
					return true;
			} else if("<=".equals(operator)) {
				if(Double.parseDouble(resultValue) <= Double.parseDouble(compareValue))
					return true;
			} else if("".equals(operator)){
				return true;
			} else {
				return null;
			}
			return false;
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
				String alarmText = doc.getString("alarmText"); //告警内容模板
				boolean triggerAlarm = false; //标识是否需要触发告警的flag
				
				//如果alarmCondition是Bson Document对象，则解析对象并执行计算，否则终止
				if(!(aco instanceof Document)) {
					log.error("监控 ["+doc.getString("jobName")+"] alarmCondition解析错误，不是合法的Bson Document");
					return;
				} else {
					Document condition = (Document)aco;
					Object factorsO = condition.get("factors");
					//如果alarmCondition.factors是Bson数组对象，则解析对象并执行计算，否则终止
					if(!(factorsO instanceof List<?>)) {
						log.error("监控 ["+doc.getString("jobName")+"] alarmCondition.factors解析错误，不是合法的Bson数组");
						return;
					} else {
						List<?> factorList = (List<?>)factorsO;
						Map<String, String> resultMap = new HashMap<String, String>(); //用于保存执行命令的结果
						for(int i=0; i<factorList.size(); i++) {
							Object factorO = factorList.get(i);
							//如果alarmCondition.factors中的元素是Bson Document对象，则解析对象并执行计算，否则终止
							if(!(factorO instanceof Document)) {
								log.error("监控 ["+doc.getString("jobName")+"] alarmCondition.factors["+i+"] 解析错误，不是合法的Bson Document");
								return;
							} else {
								Document factor = (Document)factorO;
								String collection = getCollectionName(factor.getString("collection")); //目标集合名
								if(collection == null) {
									log.error("监控 [" + doc.getString("jobName") + "] alarmCondition.factors["+i+"].collection 解析错误");
									return;
								}
								String command = factor.getString("command"); //命令，aggregate或find
								String resultKey = factor.getString("resultKey"); //返回Bson文档中结果值的key
								String key = factor.getString("key"); 
								String bson = factor.getString("bson"); //入参Bson文档
								bson = parseDate(bson);
								if(bson == null) {
									log.error("监控 [" + doc.getString("jobName") + "] alarmCondition.factors["+i+"].bson 解析错误");
									return;
								}
								Document resDoc; //执行命令返回的Bson文档
								if("aggregate".equals(command)) {
									//将bson字串解析为Bson数组，并执行aggregate操作
									List<Bson> agList = new ArrayList<Bson>();
									Object bsonList = JSON.parse(bson);
									if(!(bsonList instanceof List<?>)) {
										log.error("监控 ["+doc.getString("jobName")+"] alarmCondition.factors["+i+"].bson 解析错误，不是合法的Bson数组");
										return;
									} else {
										for(Object o : (List<?>)bsonList) {
											if(o instanceof Bson)
												agList.add((Bson)o);
										}
									}
									if(agList.size() > 0) {
										resDoc = InstanceHolder.recordMdb.getCollection(collection).aggregate(agList).allowDiskUse(true).first();
									} else {
										log.error("监控 ["+doc.getString("jobName")+"] alarmCondition.factors["+i+"].bson 解析错误，不是合法的Bson数组");
										return;
									}
								} else if("find".equals(command)) {
									//将bson字串解析为Bson Document，并执行find操作
									resDoc = InstanceHolder.recordMdb.getCollection(collection).find(Document.parse(bson)).first();
								} else {
									log.error("监控 [" + doc.getString("jobName") + "] alarmCondition.factors["+i+"].command 配置错误，不能是aggregate或find以外的值");
									return;
								}
								//从返回的Bson Document中获取结果值，并将键值对加入resultMap
								if(resDoc != null) {
									log.info("自定义监控 [" + doc.getString("jobName") + "] alarmCondition.factors["+i+"] 执行结果值：" + resDoc.get(resultKey));
									resultMap.put(key, String.valueOf(resDoc.get(resultKey)));
								}
								else {
									log.info("自定义监控 [" + doc.getString("jobName") + "] alarmCondition.factors["+i+"] 执行未找到匹配数据，结果值默认为0");
									resultMap.put(key, String.valueOf("0"));
								}
							}
						}
						String resultExpr = condition.getString("resultExpr"); //计算结果值的表达式
						String resultValue = null; //根据表达式计算出的结果值
						String compareValue = String.valueOf(condition.get("threshold")); //阈值
						boolean isSimple = false; //resultExpr表达式是简单的单一变量或是一个复杂的运算式
						//遍历resultMap的key，如果某个key值和表达式完全一致，则isSimple=true
						for(String key : resultMap.keySet()) {
							if(key.equals(resultExpr)) {
								isSimple = true;
								resultValue = resultMap.get(key);
								break;
							}
						}
						
						//isSimple==false的情形，使用parsii解析表达式并带入值计算
						if(!isSimple) {
							Scope scope = new Scope();
							for(Entry<String, String> e : resultMap.entrySet()) {
								Variable v = scope.getVariable(e.getKey());
								v.setValue(Double.parseDouble(e.getValue()));
							}
							double resDouble = Parser.parse(resultExpr, scope).evaluate();
							resultValue = new BigDecimal(resDouble).setScale(2, BigDecimal.ROUND_HALF_DOWN).toString();
						}
						
						if(resultValue == null) {
							log.error("监控 [" + doc.getString("jobName") + "] 执行异常，表达式计算结果为null，请检查alarmCondition.resultExpr和alarmCondition.factors.key配置");
							return;
						}

						String operator = condition.getString("operator");
						String compareToHistory = condition.getString("compareToHistory");
						if("no".equals(compareToHistory)) {
							//将结果与阈值比较，符合条件则将triggerAlarm置为true
							Boolean compareResult = compare(operator, compareValue, resultValue);
							if(compareResult == null) {
								log.error("自定义监控 [" + doc.getString("jobName") + "] alarmCondition.operator配置无效：" + operator);
								return;
							} else {
								triggerAlarm = compareResult;
							}
							
							//将告警内容模板中的阈值和当前值关键字替换为实际的值
							if(triggerAlarm) {
								for(Entry<String, String> e : resultMap.entrySet()) {
									alarmText = alarmText.replace("{"+e.getKey()+"}", e.getValue());
								}
								alarmText = alarmText.replace("{threshold}", compareValue).replace("{result}", resultValue);
							}
						} else {
							//将结果与历史值（前一天同一时段和一周前同一天的同一时段）比较，变化幅度大于阈值则告警
							boolean incComp = false;//与历史值比较增长率
							boolean decComp = false;//与历史之比较下降率
							if("inc".equals(compareToHistory))
								incComp = true;
							else if("dec".equals(compareToHistory))
								decComp = true;
							
							//将本次值写入历史库
							sdf = new SimpleDateFormat("yyyyMMddHHmm");
							InstanceHolder.recordMdb.getCollection("historyRecord").insertOne(new Document("monitorJobId", String.valueOf(doc.getObjectId("_id"))).append("date", sdf.format(d)).append("value", resultValue));
	
							//取得前一天同一时段和一周前同一天的同一时段的值
							String yesterday = sdf.format(new Date(d.getTime()-24*3600*1000));
							String lastWeek = sdf.format(new Date(d.getTime()-7*24*3600*1000));
							Document yDoc = InstanceHolder.recordMdb.getCollection("historyRecord").find(new Document("monitorJobId", String.valueOf(doc.getObjectId("_id"))).append("date", yesterday)).first();
							Document lwDoc = InstanceHolder.recordMdb.getCollection("historyRecord").find(new Document("monitorJobId", String.valueOf(doc.getObjectId("_id"))).append("date", lastWeek)).first();
							Double yesterdayValue = null;
							Double lastWeekValue = null;
							if(yDoc != null && yDoc.get("value") != null) {
								yesterdayValue = Double.parseDouble(String.valueOf(yDoc.get("value")));
							}
							if(lwDoc != null && lwDoc.get("value") != null) {
								lastWeekValue = Double.parseDouble(String.valueOf(lwDoc.get("value")));
							}
							int nowValue = Integer.parseInt(resultValue);//当前值int
							String historyString = "";//用于替换告警语模板的字串
							double percent = 0;//百分比阈值
							if(yesterdayValue != null || lastWeekValue != null) {
								//比较前一天值和前一周值，得出两个值中较小的与较大的
								String largerDay,lesserDay;
								double largerValue,lesserValue;
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
										Boolean compareResult = compare(operator, compareValue, String.valueOf(percent));
										if(compareResult == null) {
											log.error("自定义监控 [" + doc.getString("jobName") + "] alarmCondition.operator配置无效：" + operator);
											return;
										} else {
											triggerAlarm = compareResult;
											historyString = lesserValue + "(" + lesserDay + ")";
										}
									}
								}
								if(decComp) {
									//计算下降率，将当前值与【昨天值和上周值中较大的一个】比较
									if(nowValue < largerValue) {
										percent = (1 - (double)nowValue/(double)largerValue)*100;
										Boolean compareResult = compare(operator, compareValue, String.valueOf(percent));
										if(compareResult == null) {
											log.error("自定义监控 [" + doc.getString("jobName") + "] alarmCondition.operator配置无效：" + operator);
											return;
										} else {
											triggerAlarm = compareResult;
											historyString = largerValue + "(" + largerDay + ")";
										}
									}
								}
							}
							
							//将告警内容模板中的阈值和当前值关键字替换为实际的值
							if(triggerAlarm) {
								for(Entry<String, String> e : resultMap.entrySet()) {
									alarmText = alarmText.replace("{"+e.getKey()+"}", e.getValue());
								}
								alarmText = alarmText.replace("{threshold}", compareValue).replace("{result}", resultValue).replace("{historyValue}", historyString).replace("{percent}", new BigDecimal(percent).setScale(2, BigDecimal.ROUND_HALF_DOWN).toString());
							}
						}
					}
				}
				
				//发送告警
				if(triggerAlarm) {
					log.warn("发送监控告警：[" + alarmText + "] 至 [" + doc.getString("receivers") + "]");
					AlarmSender.sendAlarm(alarmText, doc.getString("receivers").split(","));
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
