#!/bin/bash
if [[ `which kafka-topics.sh 2>/dev/null` == "" ]];then
	echo "kafka-topics.sh is not in PATH variable, exiting abnormally."
	exit
fi

if [ "x$1" = "x" ];then
	echo "usage: kafka-create-topic.sh <kafkatopic>"
	exit
fi

kafka-topics.sh --create --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 1 --topic $1
