#!/bin/bash
curl 'localhost:9200/_mget' -d '{"docs" : [{"_index" : "logstorm", "_type" : "logentry", "_id" : "KAKfsJjoTtueA0ywxVmMYA"}]}'
