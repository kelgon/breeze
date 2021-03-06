#!/bin/sh

jarpath=""

for _FNAME in lib/*.jar
  do
  jarpath=$jarpath:$_FNAME
done

CLASSPATH=$CLASSPATH:$jarpath
export CLASSPATH
echo "starting breeze-consumer..."

java -Xms64m -Xmx128m -classpath $CLASSPATH com.asiainfo.breeze.consumer.ConsumerRunner &

sleep 1

tail -f logs/breeze.log
