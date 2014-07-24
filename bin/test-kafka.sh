#!/bin/bash

THIS="$0"
THIS_DIR=`dirname "$THIS"`
HOLT_HOME=`cd "$THIS_DIR/.." ; pwd`

topic=$1

if [ "x$1" = "x" ];then
	topic="test-kafka"
fi

KAFKA_TOPIC=""
for f in `kafka-topics.sh --list --zookeeper 127.0.0.1:2181`; do
  if [ "${topic}" = "$f" ];then
  	KAFKA_TOPIC=${topic}
  	break
  fi
done

if [ "x$KAFKA_TOPIC" = "x" ];then
	#create topic
	kafka-topics.sh --create --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 1 --topic ${topic}
	KAFKA_TOPIC=${topic}
fi

#list topic
kafka-topics.sh --list --zookeeper localhost:2181
#produce messages
kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic ${KAFKA_TOPIC}

#test-kafka consumer
#kafka-console-consumer.sh --topic test-kafka --zookeeper 127.0.0.1:2181 --from-beginning

