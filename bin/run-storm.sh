#!/bin/bash

#bin/run-storm.sh --topic test-log2

# some directories
THIS="$0"
THIS_DIR=`dirname "$THIS"`
HOLT_HOME=`cd "$THIS_DIR/.." ; pwd`

HOLT_JAR=$HOLT_HOME/target/*-jar-with-dependencies.jar

#run storm-jar
set -x
storm jar ${HOLT_JAR} ca.dealsaccess.holt.driver.HoltDriver "$@"
