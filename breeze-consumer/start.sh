#!/bin/sh

jarpath=""

for _FNAME in /home/web/breeze-consumer/lib/*.jar
  do
  jarpath=$jarpath:$_FNAME
done

CLASSPATH=$CLASSPATH:$jarpath
export CLASSPATH
echo "starting breeze-consumer..."

java -Xms64m -Xmx128m -classpath $CLASSPATH com.asiainfo.breeze.consumer.ConsumerRunner &

tail -f /home/web/breeze-consumer/logs/breeze.log

