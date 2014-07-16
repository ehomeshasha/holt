#!/bin/bash

#examples
#bin/flume-kafka-start.sh grouplens-ml-100k 'cat /home/hadoop-user/scout_workspace/holt/src/test/resources/grouplens-ml-100k/ua.base'
#bin/flume-kafka-start.sh test-log2 'tail -f -n +1 /home/hadoop-user/scout_workspace/holt/test.log'
#bin/flume-kafka-start.sh access-log 'tail -f -n +1 /var/log/httpd/access_log'

if [[ "x$1" = "x" || "x$2" = "x" ]];then
	echo "usage: holt-task-start.sh <kafkatopic> <exec-command>"
	exit
fi


kafka_topics="kafka-topics.sh"
if [[ `which ${kafka_topics} 2>/dev/null` == "" ]];then
	echo "${kafka_topics} is not in PATH variable, exiting abnormally."
	exit
fi

JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx1000m

# some directories
THIS="$0"
THIS_DIR=`dirname "$THIS"`
HOLT_HOME=`cd "$THIS_DIR/.." ; pwd`

# some Java parameters
if [ "$HOLT_JAVA_HOME" != "" ]; then
  #echo "run java in $HOLT_JAVA_HOME"
  JAVA_HOME=$HOLT_JAVA_HOME
fi

if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

if [ "x$HOLT_CONF_DIR" = "x" ]; then
  if [ -d $HOLT_HOME/src/conf ]; then
    HOLT_CONF_DIR=$HOLT_HOME/src/conf
  else
    if [ -d $HOLT_HOME/conf ]; then
      HOLT_CONF_DIR=$HOLT_HOME/conf
    else
      echo No HOLT_CONF_DIR found
    fi
  fi
fi

HOLT_JAR=""
for f in $HOLT_HOME/target/*-SNAPSHOT.jar; do
  if [ "x${HOLT_JAR}" = "x" ];then
  	HOLT_JAR=$f
  else
  	HOLT_JAR=${HOLT_JAR}:$f;
  fi
done

if [[ "$KAFKA_HOME" = "" ]];then
	echo '$KAFKA_HOME is not set, exiting abnormally'
	exit
fi


KAFKA_TOPIC=""
for f in `kafka-topics.sh --list --zookeeper 127.0.0.1:2181`; do
  if [ "$1" = "$f" ];then
  	#delete exist topic
	#${HOLT_HOME}/bin/kafka-delete-topic.sh $1
	KAFKA_TOPIC=$1
	break
  fi
done

if [ "x$KAFKA_TOPIC" = "x" ];then
	read -p "$1 is not exist in kafka topic list, create it?" yn
	case $yn in
	    [Yy]* ) kafka-topics.sh --create --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 1 --topic $1;KAFKA_TOPIC=$1;;
	    [Nn]* ) exit;;
	    * ) echo "Please answer yes or no.";;
	esac
fi

#delete all existing flume-ng first
ps -ef | grep "$KAFKA_HOME/libs/" | awk {'print $2'} | xargs kill -9



#set flume-kafka-conf.properties and log4j.properties
FORMAT_DATE=`date +%Y.%m.%d-%H:%M:%S`
LOGFILE="flume-kafka-start-$1-${FORMAT_DATE}.log"
DATE_DIR=`date +%Y%m%d`

mkdir -p ${HOLT_HOME}/holt-logs/${DATE_DIR}

sed -i "s;\(log4j\.appender\.R\.File[[:space:]]*=[[:space:]]*\).*;\1${HOLT_HOME}/holt-logs/${DATE_DIR}/${LOGFILE};" ${HOLT_HOME}/conf/log4j.properties
sed -i -e "s;\(producer\.sinks\.r\.custom\.topic\.name[[:space:]]*=[[:space:]]*\).*;\1$1;" -e "s;\(producer\.sources\.s\.command[[:space:]]*=[[:space:]]*\).*;\1$2;" ${HOLT_HOME}/conf/flume-kafka-conf.properties

#data moves from flume to kafka

echo "flume-ng agent -n producer -f ${HOLT_HOME}/conf/flume-kafka-conf.properties --classpath $KAFKA_HOME/libs/\*:${HOLT_JAR}"
read -p "moves data in $2 to kafka using flume?" yn
case $yn in
	[Yy]* ) flume-ng agent -n producer -f $HOLT_HOME/conf/flume-kafka-conf.properties --classpath $KAFKA_HOME/libs/\*:${HOLT_JAR};;
	[Nn]* ) exit;;
	* ) echo "Please answer yes or no.";;
esac

