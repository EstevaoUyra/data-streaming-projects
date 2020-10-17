#!/bin/bash

KAFKA_HOME=/Users/estevao.uyra/dev/personal/kafka
PORT=9092

PATH=$PATH:"${KAFKA_HOME}"/bin

function start_server {
  kafka-server-start.sh "${KAFKA_HOME}"/config/server.properties
}

function start_zookeeper {
  zookeeper-server-start.sh "${KAFKA_HOME}"/config/zookeeper.properties
}

function run_kafka_background {
  echo Warning! Closing the terminal will not stop the process.
  start_server & \
  start_zookeeper &
}

function kconsume {
  kafka-console-consumer.sh \
  --bootstrap-server localhost:"${PORT}" \
  --topic "$1" \
  --from-beginning
}
