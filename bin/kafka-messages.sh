#!/bin/bash

if [ "x$1" = "x" ];then
	echo "usage: kafka-messsages.sh <kafkatopic>"
	exit
fi

kafka-console-consumer.sh --zookeeper 127.0.0.1:2181 --topic $1 --from-beginning
