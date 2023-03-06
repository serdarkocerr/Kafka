#!/bin/bash

TOPICS=$(/opt/kafka/bin/kafka-topics.sh --zookeeper 10.152.16.201:2181 --list )

for T in $TOPICS
do
  if [ "$T" != "__consumer_offsets" ]; then
   /opt/infra/kafka/bin/kafka-topics.sh --zookeeper 10.152.16.201:2181 --delete --topic $T
  fi
done
