#!/bin/bash

export FA_HOME=${FA_HOME:-/opt/flink-autoscaler/active}
export FA_CONF_DIR=${FA_CONF_DIR:-/etc/flink-confs/flink-autoscaler/active/conf}
export FA_LOG_DIR=${FA_LOG_DIR:-/data/flink/log/flink-autoscaler}
export FM_CONF_YAML=${FA_CONF_DIR}/fi-conf.yaml
export HADOOP_CONF_DIR="/etc/hadoop-client"
export HADOOP_USER_RPCPASSWORD=$(grep 'hadoop.user.rpcpassword' ${FM_CONF_YAML} | awk '{print $2}')

echo "FA_HOME: ${FA_HOME}"
echo "FA_CONF_DIR: ${FA_CONF_DIR}"
echo "FA_LOG_DIR: ${FA_LOG_DIR}"
echo "HADOOP_USER_RPCPASSWORD: ${HADOOP_USER_RPCPASSWORD}"

LOG_CONF_XML=${FA_CONF_DIR}/logback.xml
LOG_FILE=${FA_LOG_DIR}/flink-autoscaler
FA_HEAP_SIZE=${FA_HEAP_SIZE:-8G}

# set java run
FA_TIMEZONE="-Duser.timezone=Asia/Singapore"
FA_JVM_ARGS="-XX:+UseG1GC -Xms${FA_HEAP_SIZE} -Xmx${FA_HEAP_SIZE}"
FA_CLASSPATH="${FA_HOME}/lib/*:${FA_CONF_DIR}"
LOG_SETTING="-Dlogging.config=file:${LOG_CONF_XML} -Dlog.file=${LOG_FILE}"

echo "FA_JVM_ARGS: ${FA_JVM_ARGS}"
echo "FA_CLASSPATH: ${FA_CLASSPATH}"
echo "LOG_SETTING: ${LOG_SETTING}"

exec /opt/jdk-17.0.5/bin/java ${FA_TIMEZONE} ${FA_JVM_ARGS} ${LOG_SETTING} -classpath "${FA_CLASSPATH}" org.apache.flink.autoscaler.standalone.StandaloneAutoscalerEntrypoint

# /opt/jdk-17.0.5/bin/java -classpath ./flink-autoscaler-standalone-1.8-SNAPSHOT.jar org.apache.flink.autoscaler.standalone.StandaloneAutoscalerEntrypoint
