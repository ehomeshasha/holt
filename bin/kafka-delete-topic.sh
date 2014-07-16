#!/bin/bash

if [ "x$1" = "x" ];then
	echo "usage: kafka-delete-topic.sh <kafkatopic>"
	exit
fi

#rm -rf /tmp/kafka-logs/$1-*

if [ "x$1" = "x-all" ];then
	for f in `kafka-topics.sh --list --zookeeper 127.0.0.1:2181`; do
		kafka-run-class.sh kafka.admin.DeleteTopicCommand --topic $f --zookeeper 127.0.0.1	
	done
else
	kafka-run-class.sh kafka.admin.DeleteTopicCommand --topic $1 --zookeeper 127.0.0.1
fi
