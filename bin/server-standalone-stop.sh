#!/bin/bash

ps -ef | grep "zookeeper.properties" | awk {'print $2'} | xargs kill -9
ps -ef | grep "server.properties" | awk {'print $2'} | xargs kill -9
ps -ef | grep "storm" | awk {'print $2'} | xargs kill -9
ps -ef | grep "$KAFKA_HOME/libs/" | awk {'print $2'} | xargs kill -9
