#!/bin/bash

#Valid program names are:
#  counterJob: : Display message from kafka spout and count them.
#  logStatsJob: : analysis log data from redis to generate statistic results, then save them to redis.
#  saveRedisJob: : get message from kafka spout and save to the redis.



#bin/run-storm.sh counterJob --topic test-log
#bin/run-storm.sh saveRedisJob --topic access-log

# some directories
THIS="$0"
THIS_DIR=`dirname "$THIS"`
HOLT_HOME=`cd "$THIS_DIR/.." ; pwd`

HOLT_JAR=$HOLT_HOME/target/*-jar-with-dependencies.jar

#run storm-jar
set -x
storm jar ${HOLT_JAR} ca.dealsaccess.holt.driver.HoltDriver "$@"
