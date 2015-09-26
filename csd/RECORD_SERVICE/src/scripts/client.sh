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
  echo "$timestamp: $1"
}

# Replaces all occurrences of $1 with $2 in file $3, escaping $1 and $2 as necessary.
function sed_replace {
  sed -i "s/$(echo $1 | sed -e 's/\([[\/.*]\|\]\)/\\&/g')/$(echo $2 | sed -e 's/[\/&]/\\&/g')/g" $3
}

log "CMD: $CMD"
log "PLANNER_CONF_FILE: $PLANNER_CONF_FILE"
log "RECORDSERVICE_CONF_FILE: $RECORDSERVICE_CONF_FILE"
log "CONF_DIR: $CONF_DIR"
log "DIRECTORY_NAME: $DIRECTORY_NAME"
PLANNER_FILE="${CONF_DIR}/${DIRECTORY_NAME}/${PLANNER_CONF_FILE}"
log "PLANNER_FILE: $PLANNER_FILE"
PW_FILE="${CONF_DIR}/${DIRECTORY_NAME}/${PW_CONF_FILE}"
log "PW_FILE: $PW_FILE"

# As CM only copies $HADOOP_CONF_DIR to /etc/hadoop/conf.cloudera.record_service*,
# we should also copy YARN / MAPREDUCE conf into $HADOOP_CONF_DIR.
YARN_CONF_DIR="${CONF_DIR}/yarn-conf"
HADOOP_CONF_DIR="${CONF_DIR}/hadoop-conf"
log "YARN_CONF_DIR: $YARN_CONF_DIR"
log "HADOOP_CONF_DIR: $HADOOP_CONF_DIR"

# Copy yarn-conf under HADOOP config.
# Copy mapreduce-conf under HADOOP config, if yarn-conf is not there.
if [ -d "$YARN_CONF_DIR" ]; then
  log "Copy $YARN_CONF_DIR to $HADOOP_CONF_DIR"
  cp $YARN_CONF_DIR/* $HADOOP_CONF_DIR
fi

# Because of OPSAPS-25695, we need to fix HADOOP config ourselves.
log "CDH_MR2_HOME: $CDH_MR2_HOME"
log "HADOOP_CLASSPATH: $HADOOP_CLASSPATH"
for i in "$HADOOP_CONF_DIR"/*; do
  log "i: $i"
  sed_replace "{{CDH_MR2_HOME}}" "$CDH_MR2_HOME" "$i"
  sed_replace "{{HADOOP_CLASSPATH}}" "$HADOOP_CLASSPATH" "$i"
  sed_replace "{{JAVA_LIBRARY_PATH}}" "" "$i"
done

# Adds a xml config to $RECORDSERVICE_CONF_FILE
add_to_recordservice_conf() {
  FILE=`find $CONF_DIR/$DIRECTORY_NAME -name $RECORDSERVICE_CONF_FILE`
  log "Add $1:$2 to $FILE"
  CONF_END="</configuration>"
  NEW_PROPERTY="<property><name>$1</name><value>$2</value></property>"
  TMP_FILE=$CONF_DIR/$DIRECTORY_NAMEtmp-con-file
  cat $FILE | sed "s#$CONF_END#$NEW_PROPERTY#g" > $TMP_FILE
  cp $TMP_FILE $FILE
  rm -f $TMP_FILE
  echo $CONF_END >> $FILE
}

CONF_KEY=recordservice.planner.hostports
CONF_VALUE=
copy_planner_hostports_from_file() {
  log "copy from $1"
  if [ -f $PLANNER_FILE ]; then
    for line in $(cat $1)
    do
      log "line $line"
      if [[ $line == *":"*"="* ]]; then
        PLANNER_HOST=${line%:*}
        PLANNER_PORT=${line##*=}
        log "add $PLANNER_HOST:$PLANNER_PORT"
        CONF_VALUE=$CONF_VALUE,$PLANNER_HOST:$PLANNER_PORT
      fi
    done
  fi
}

case $CMD in
  (deploy)
    log "Deploy client configuration"
    copy_planner_hostports_from_file $PLANNER_FILE
    copy_planner_hostports_from_file $PW_FILE
    log "CONF_KEY: $CONF_KEY"
    log "CONF_VALUE: $CONF_VALUE"
    if [ -n "$CONF_VALUE" ]; then
      # remove the first ','
      CONF_VALUE=${CONF_VALUE:1}
      add_to_recordservice_conf $CONF_KEY $CONF_VALUE
    fi
  ;;
  (*)
    log "Don't understand [$CMD]"
  ;;
esac
