#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex

CMD=$1

function log {
  timestamp=$(date)
  echo "${timestamp}: $1"
}

# Replaces all occurrences of $1 with $2 in file $3, escaping $1 and $2 as necessary.
function sed_replace {
  sed -i "s/$(echo $1 | sed -e 's/\([[\/.*]\|\]\)/\\&/g')/$(echo $2 | sed -e 's/[\/&]/\\&/g')/g" $3
}

log "CMD: ${CMD}"
log "PLANNER_CONF_FILE: ${PLANNER_CONF_FILE}"
log "RECORDSERVICE_CONF_FILE: ${RECORDSERVICE_CONF_FILE}"
log "CONF_DIR: ${CONF_DIR}"
log "DIRECTORY_NAME: ${DIRECTORY_NAME}"
RECORDSERVICE_CONF_DIR="${CONF_DIR}/${DIRECTORY_NAME}"
log "RECORDSERVICE_CONF_DIR: ${RECORDSERVICE_CONF_DIR}"
PLANNER_FILE="${RECORDSERVICE_CONF_DIR}/${PLANNER_CONF_FILE}"
log "PLANNER_FILE: ${PLANNER_FILE}"
PW_FILE="${RECORDSERVICE_CONF_DIR}/${PW_CONF_FILE}"
log "PW_FILE: ${PW_FILE}"
SPARK_FILE="${RECORDSERVICE_CONF_DIR}/${SPARK_CONF_FILE}"
log "SPARK_FILE: ${SPARK_FILE}"
log "HDFS_CONFIG: ${HDFS_CONFIG}"

# As CM only copies $RECORDSERVICE_CONF_DIR to /etc/recordservice/conf.cloudera.record_service,
# we should also copy YARN / HADOOP conf into RECORDSERVICE_CONF_DIR.
YARN_CONF_DIR="${CONF_DIR}/yarn-conf"
HADOOP_CONF_DIR="${CONF_DIR}/hadoop-conf"
log "YARN_CONF_DIR: ${YARN_CONF_DIR}"
log "HADOOP_CONF_DIR: ${HADOOP_CONF_DIR}"

# Copy yarn-conf under recordservice-conf.
# Copy hadoop-conf under recordservice-conf, if yarn-conf is not there.
if [ -d "${YARN_CONF_DIR}" ]; then
  log "Copy ${YARN_CONF_DIR} to ${RECORDSERVICE_CONF_DIR}"
  cp ${YARN_CONF_DIR}/* ${RECORDSERVICE_CONF_DIR}
elif [ -d "${HADOOP_CONF_DIR}" ]; then
  log "Copy ${HADOOP_CONF_DIR} to ${RECORDSERVICE_CONF_DIR}"
  cp ${HADOOP_CONF_DIR}/* ${RECORDSERVICE_CONF_DIR}
fi

# Because of OPSAPS-25695, we need to fix HADOOP config ourselves.
log "CDH_MR2_HOME: ${CDH_MR2_HOME}"
log "HADOOP_CLASSPATH: ${HADOOP_CLASSPATH}"
for i in "${RECORDSERVICE_CONF_DIR}"/*; do
  log "i: $i"
  sed_replace "{{CDH_MR2_HOME}}" "${CDH_MR2_HOME}" "$i"
  sed_replace "{{HADOOP_CLASSPATH}}" "${HADOOP_CLASSPATH}" "$i"
  sed_replace "{{JAVA_LIBRARY_PATH}}" "" "$i"
done

# Adds a xml config to RECORDSERVICE_CONF_FILE
add_to_recordservice_conf() {
  FILE=`find ${RECORDSERVICE_CONF_DIR} -name ${RECORDSERVICE_CONF_FILE}`
  log "Add $1:$2 to ${FILE}"
  CONF_END="</configuration>"
  NEW_PROPERTY="<property><name>$1</name><value>$2</value></property>"
  TMP_FILE="${RECORDSERVICE_CONF_DIR}/tmp-conf-file"
  cat ${FILE} | sed "s#${CONF_END}#${NEW_PROPERTY}#g" > ${TMP_FILE}
  cp ${TMP_FILE} ${FILE}
  rm -f ${TMP_FILE}
  echo ${CONF_END} >> ${FILE}
}

# Adds a properties conf to SPARK_FILE
add_to_spark_conf() {
  log "ADD $1:$2 to ${SPARK_FILE}"
  NEW_PROPERTY="$1=$2"
  TMP_FILE="${RECORDSERVICE_CONF_DIR}/tmp-spark-conf-file"
  cat ${SPARK_FILE} > ${TMP_FILE}
  echo ${NEW_PROPERTY} >> ${TMP_FILE}
  log "Add prefix spark. in each line"
  sed -i -e 's/^/spark./' ${TMP_FILE}
  cp ${TMP_FILE} ${SPARK_FILE}
  rm -f ${TMP_FILE}
}

# Adds a xml config to hdfs-site.xml
add_to_hdfs_site() {
  FILE=`find ${RECORDSERVICE_CONF_DIR} -name hdfs-site.xml`
  CONF_END="</configuration>"
  NEW_PROPERTY="<property><name>$1</name><value>$2</value></property>"
  TMP_FILE="${CONF_DIR}/tmp-hdfs-site"
  cat ${FILE} | sed "s#${CONF_END}#${NEW_PROPERTY}#g" > ${TMP_FILE}
  cp ${TMP_FILE} ${FILE}
  rm -f ${TMP_FILE}
  echo ${CONF_END} >> ${FILE}
}

# Append to hdfs-site.xml if HDFS_CONFIG is not empty.
append_to_hdfs_site() {
  if [[ -n ${HDFS_CONFIG} ]]; then
    FILE=`find ${RECORDSERVICE_CONF_DIR} -name hdfs-site.xml`
    CONF_END="</configuration>"
    TMP_FILE="${CONF_DIR}/tmp-hdfs-site"
    cat ${FILE} | sed "s#${CONF_END}#${HDFS_CONFIG}#g" > ${TMP_FILE}
    cp ${TMP_FILE} ${FILE}
    rm -f ${TMP_FILE}
    echo ${CONF_END} >> ${FILE}
  fi
}


CONF_KEY=recordservice.planner.hostports
CONF_VALUE=
copy_planner_hostports_from_file() {
  log "copy from $1"
  if [ -f ${PLANNER_FILE} ]; then
    for line in $(cat $1)
    do
      log "line ${line}"
      if [[ ${line} == *":"*"="* ]]; then
        PLANNER_HOST=${line%:*}
        PLANNER_PORT=${line##*=}
        log "add ${PLANNER_HOST}:${PLANNER_PORT}"
        CONF_VALUE="${CONF_VALUE},${PLANNER_HOST}:${PLANNER_PORT}"
      fi
    done
  fi
}

case $CMD in
  (deploy)
    log "Deploy client configuration"
    copy_planner_hostports_from_file ${PLANNER_FILE}
    copy_planner_hostports_from_file ${PW_FILE}
    log "CONF_KEY: ${CONF_KEY}"
    log "CONF_VALUE: ${CONF_VALUE}"
    if [ -n "${CONF_VALUE}" ]; then
      # remove the first ','
      CONF_VALUE=${CONF_VALUE:1}
      add_to_recordservice_conf ${CONF_KEY} ${CONF_VALUE}
      add_to_spark_conf ${CONF_KEY} ${CONF_VALUE}
    fi
    # Add zk quorum to hdfs-site.xml
    add_to_hdfs_site recordservice.zookeeper.connectString ${ZK_QUORUM}
    # FIXME this is not secure.
    add_to_hdfs_site recordservice.zookeeper.acl world:anyone:cdrwa
    # Enable short circuit read in hdfs-site.xml.
    add_to_hdfs_site dfs.client.read.shortcircuit true
    # Append HDFS_CONFIG to hdfs-site.xml.
    # This can overwrite the original value.
    append_to_hdfs_site
  ;;
  (*)
    log "Don't understand [$CMD]"
  ;;
esac
