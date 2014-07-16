#!/bin/bash
# Author: zzy
# Date: 2014.7.7
# Description: startup server file

zookeeper_server_start="zookeeper-server-start.sh"
kafka_server_start="kafka-server-start.sh"

if [[ "$KAFKA_HOME" = "" ]];then
	echo '$KAFKA_HOME is not set, exiting abnormally'
	exit
fi

if [[ `which ${zookeeper_server_start} 2>/dev/null` == "" ]];then
	echo "${zookeeper_server_start} is not in PATH variable, exiting abnormally."
	exit
fi
if [[ `which ${kafka_server_start} 2>/dev/null` == "" ]];then
	echo "${kafka_server_start} is not in PATH variable, exiting abnormally."
	exit
fi
if [[ `which storm 2>/dev/null` == "" ]];then
	echo "storm is not in PATH variable, exiting abnormally."
	exit
fi

#start zookeeper
nohup zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &
#start kafka server
nohup kafka-server-start.sh $KAFKA_HOME/config/server.properties &


#start storm nimbus
nohup storm nimbus &
#start storm supervisor
nohup storm supervisor &
#start storm ui
nohup storm ui &
