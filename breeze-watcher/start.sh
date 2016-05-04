#!/bin/sh

jarpath=""

for _FNAME in lib/*.jar
  do
  jarpath=$jarpath:$_FNAME
done

CLASSPATH=$CLASSPATH:$jarpath
export CLASSPATH
echo "starting breeze-watcher..."

java -Xms64m -Xmx128m -classpath $CLASSPATH com.asiainfo.breeze.watcher.WatcherRunner &

sleep 1

tail -f logs/breeze.log

