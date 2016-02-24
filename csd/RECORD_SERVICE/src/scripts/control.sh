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

# Adds a xml config to hdfs-site.xml
add_to_hdfs_site() {
  FILE=`find ${HADOOP_CONF_DIR} -name hdfs-site.xml`
  CONF_END="</configuration>"
  NEW_PROPERTY="<property><name>$1</name><value>$2</value></property>"
  TMP_FILE="${CONF_DIR}/tmp-hdfs-site"
  cat ${FILE} | sed "s#${CONF_END}#${NEW_PROPERTY}#g" > ${TMP_FILE}
  cp ${TMP_FILE} ${FILE}
  rm -f ${TMP_FILE}
  echo ${CONF_END} >> ${FILE}
}

check_exist_wildcard() {
  if [ "$(echo $1)" != "$1" ]; then
    return 0
  else
    return 1
  fi
}

# Check if DIGEST-MD5 is installed when Kerberos is enabled
# TODO: user may install this at some unconventional place and set it via
# the flag 'sasl_path'. We should handle this.
check_digest_md5() {
  if env | grep -q ^RECORD_SERVICE_PRINCIPAL=; then
    if ! check_exist_wildcard "/usr/lib/sasl2/libdigestmd5*" && \
       ! check_exist_wildcard "/usr/lib64/sasl2/libdigestmd5*" && \
       ! check_exist_wildcard "/usr/local/lib/sasl2/libdigestmd5*" && \
       ! check_exist_wildcard "/usr/lib/x86_64-linux-gnu/sasl2/libdigestmd5*"; then
      log "Kerberos is enabled but couldn't find DIGEST-MD5 library on the system"
      exit 1
    fi
  fi
}

# RECORDSERVICE_HOME is provided in the recordservice parcel's env script.
RECORD_SERVICE_BIN_HOME=${RECORDSERVICE_HOME}/../../bin
log "RECORD_SERVICE_BIN_HOME: ${RECORD_SERVICE_BIN_HOME}"

if [ "${DEBUG}" == "true" ]; then
  log "running debug binaries"
  export RECORDSERVICE_BIN=${RECORDSERVICE_BIN:-$RECORDSERVICE_HOME/sbin-debug}
fi
log "RECORDSERVICE_BIN: ${RECORDSERVICE_BIN}"

log "CMD: ${CMD}"


export HIVE_CONF_DIR=${CONF_DIR}/hive-conf
# Use yarn-conf as HADOOP_CONF_DIR
# Use hadoop-conf if yarn-conf is not there
export HADOOP_CONF_DIR=${CONF_DIR}/yarn-conf
if [ ! -d "${HADOOP_CONF_DIR}" ]; then
  HADOOP_CONF_DIR="${CONF_DIR}/hadoop-conf"
  if [ ! -d "${HADOOP_CONF_DIR}" ]; then
    log "No Hadoop configuration found."
    exit 1
  fi
fi

export USER=recordservice
export SENTRY_CONF_DIR=${CONF_DIR}/sentry-conf

env

log "HADOOP_CONF_DIR: ${HADOOP_CONF_DIR}"
log "HIVE_CONF_DIR: ${HIVE_CONF_DIR}"
log "USER: ${USER}"
log "RECORDSERVICE_CONF_DIR: ${RECORDSERVICE_CONF_DIR}"

KEYTAB_FILE=${HADOOP_CONF_DIR}/../record_service.keytab

# The following parameters are provided in descriptor/service.sdl.
log "Starting RecordService"
log "recordservice_planner_port: ${PLANNER_PORT}"
log "recordservice_worker_port: ${WORKER_PORT}"
log "log_filename: ${LOG_FILENAME}"
log "hostname: ${HOSTNAME}"
log "recordservice_webserver_port: ${WEBSERVER_PORT}"
log "webserver_doc_root: ${RECORDSERVICE_HOME}"
log "log_dir: ${LOG_DIR}"
log "principal: ${RECORD_SERVICE_PRINCIPAL}"
log "keytab_file: ${KEYTAB_FILE}"
log "v: ${V}"
log "minidump_directory: ${MINIDUMP_DIRECTORY}"
log "mem_limit: ${MEM_LIMIT}"
log "sentry_config: ${SENTRY_CONFIG}"
log "advanced_config: ${ADVANCED_CONFIG}"
log "hdfs_config: ${HDFS_CONFIG}"

# Add zk quorum to hdfs-site.xml
add_to_hdfs_site recordservice.zookeeper.connectString ${ZK_QUORUM}
# Enable short circuit read in hdfs-site.xml
add_to_hdfs_site dfs.client.read.shortcircuit true

# Append to hdfs-site.xml if HDFS_CONFIG is not empty.
# This can overwrite the original value.
if [[ -n ${HDFS_CONFIG} ]]; then
  FILE=`find ${HADOOP_CONF_DIR} -name hdfs-site.xml`
  CONF_END="</configuration>"
  TMP_FILE="${CONF_DIR}/tmp-hdfs-site"
  cat ${FILE} | sed "s#${CONF_END}#${HDFS_CONFIG}#g" > ${TMP_FILE}
  cp ${TMP_FILE} ${FILE}
  rm -f ${TMP_FILE}
  echo ${CONF_END} >> ${FILE}
fi

SENTRY_CONFIG_FILE=
# Use the sentry client config: sentry-site.xml under ${CONF_DIR}/sentry-conf.
if [[ -f ${SENTRY_CONF_DIR}/sentry-site.xml ]]; then
  SENTRY_CONFIG_FILE=${SENTRY_CONF_DIR}/sentry-site.xml
  log "SENTRY_CONFIG_FILE: ${SENTRY_CONFIG_FILE}"
fi

# Append to sentry-site.xml if SENTRY_CONFIG is not empty.
if [[ -n ${SENTRY_CONFIG} ]]; then
  # create sentry-site.xml under ${CONF_DIR} if ${SENTRY_CONFIG_FILE} is empty.
  if [[ ! -n ${SENTRY_CONFIG_FILE} ]]; then
    TMP_FILE="${CONF_DIR}/sentry-site.xml"
    touch ${TMP_FILE}
    echo "<configuration>" > ${TMP_FILE}
    echo "</configuration>" >> ${TMP_FILE}
    SENTRY_CONFIG_FILE=${TMP_FILE}
    log "SENTRY_CONFIG_FILE: ${SENTRY_CONFIG_FILE}"
  fi
  CONF_END="</configuration>"
  TMP_FILE="${CONF_DIR}/tmp-sentry-site"
  cat ${SENTRY_CONFIG_FILE} | sed "s#${CONF_END}#${SENTRY_CONFIG}#g" > ${TMP_FILE}
  cp ${TMP_FILE} ${SENTRY_CONFIG_FILE}
  rm -f ${TMP_FILE}
  echo ${CONF_END} >> ${SENTRY_CONFIG_FILE}
fi

ARGS="\
  -log_filename=${LOG_FILENAME} \
  -hostname=${HOSTNAME} \
  -recordservice_webserver_port=${WEBSERVER_PORT} \
  -webserver_doc_root=${RECORDSERVICE_HOME} \
  -log_dir=${LOG_DIR} \
  -abort_on_config_error=false \
  -lineage_event_log_dir=${LOG_DIR}/lineage \
  -audit_event_log_dir=${LOG_DIR}/audit \
  -profile_log_dir=${LOG_DIR}/profiles/ \
  -v=${V} \
  -minidump_path=${MINIDUMP_DIRECTORY} \
  -mem_limit=${MEM_LIMIT} \
  -sentry_config=${SENTRY_CONFIG_FILE} \
  "
if env | grep -q ^RECORD_SERVICE_PRINCIPAL=
then
  log "Starting kerberized cluster"
  ARGS=${ARGS}"\
    -principal=${RECORD_SERVICE_PRINCIPAL} \
    -keytab_file=${KEYTAB_FILE} \
    "
fi

if [[ -n ${ADVANCED_CONFIG} ]];
then
  log "Add advanced config:${ADVANCED_CONFIG}"
  ARGS="${ARGS} ${ADVANCED_CONFIG}"
fi

# Enable core dumping if requested.
if [ "${ENABLE_CORE_DUMP}" == "true" ]; then
  # The core dump directory should already exist.
  if [ -z "${CORE_DUMP_DIRECTORY}" -o ! -d "${CORE_DUMP_DIRECTORY}" ]; then
    log "Could not find core dump directory ${CORE_DUMP_DIRECTORY}, exiting"
    exit 1
  fi
  # It should also be writable.
  if [ ! -w "${CORE_DUMP_DIRECTORY}" ]; then
    log "Core dump directory ${CORE_DUMP_DIRECTORY} is not writable, exiting"
    exit 1
  fi

  ulimit -c unlimited
  cd "${CORE_DUMP_DIRECTORY}"
  STATUS=$?
  if [ ${STATUS} != 0 ]; then
    log "Could not change to core dump directory to ${CORE_DUMP_DIRECTORY}, exiting"
    exit ${STATUS}
  fi
fi

case ${CMD} in
  (start_planner_worker)
    log "Starting recordserviced running planner and worker services"
    check_digest_md5
    exec ${RECORD_SERVICE_BIN_HOME}/recordserviced ${ARGS} \
      -recordservice_planner_port=${PLANNER_PORT} \
      -recordservice_worker_port=${WORKER_PORT}
  ;;

  (start_planner)
    log "Starting recordserviced running planner service"
    check_digest_md5
    exec ${RECORD_SERVICE_BIN_HOME}/recordserviced ${ARGS} \
      -recordservice_planner_port=${PLANNER_PORT} \
      -recordservice_worker_port=0
  ;;

  (start_worker)
    log "Starting recordserviced running worker service"
    check_digest_md5
    exec ${RECORD_SERVICE_BIN_HOME}/recordserviced ${ARGS} \
      -recordservice_planner_port=0 \
      -recordservice_worker_port=${WORKER_PORT}
  ;;

  (stopAll)
    log "Stopping recordserviced"
    exec killall -w recordserviced
    ;;

  (*)
    log "Don't understand [${CMD}]"
    ;;
esac
