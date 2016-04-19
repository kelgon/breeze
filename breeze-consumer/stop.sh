#!/bin/sh

if [ $# -lt 1 ]
then
  echo "Usage: stop.sh [procedure_name]"
  echo "Example: ./stop.sh breeze-consumer-1"
  exit 1
fi


PROCESS=`ps -ef|grep $1|grep "java"|grep -v grep|grep -v PPID|awk '{ print $2}'`
for i in $PROCESS
do
  echo "stopping $1 process [ $i ]"
  kill $i
done


tail -f /home/web/breeze-consumer/logs/breeze.log

