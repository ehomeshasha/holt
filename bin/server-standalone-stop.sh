#!/bin/bash


#stop kafka server
${KAFKA_HOME}/bin/kafka-server-stop.sh
#ps ax | grep -i 'kafka\.Kafka' | grep java | grep -v grep | awk '{print $1}' | xargs kill -SIGINT

#stop cassandra opscenter
echo zzy8945620 | sudo -u root -S service opscenterd stop 

#stop cassandra
set -x
echo zzy8945620 | sudo -u root -S /etc/init.d/cassandra stop 

#stop elasticsearch
ps -ef | grep "elasticsearch" | grep -v grep | awk {'print $2'} | xargs kill -15

#stop storm nimus, supervisor, ui
ps -ef | grep "storm" | grep -v grep | awk {'print $2'} | xargs kill -15

#start redis
ps -ef | grep "redis-server" | grep -v grep | awk {'print $2'} | xargs kill -15


#stop zookeeper server
#${KAFKA_HOME}/bin/zookeeper-server-stop.sh
ps ax | grep -i 'zookeeper' | grep -v grep | awk '{print $1}' | xargs kill -15

#stop flume agent
ps -ef | grep "flume-kafka-conf.properties" | grep -v grep | awk {'print $2'} | xargs kill -SIGINT

