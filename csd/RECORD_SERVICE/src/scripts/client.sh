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

log "CMD: $CMD"
log "PLANNER_CONF_FILE: $PLANNER_CONF_FILE"
log "RECORDSERVICE_CONF_FILE: $RECORDSERVICE_CONF_FILE"
log "CONF_DIR: $CONF_DIR"
log "DIRECTORY_NAME: $DIRECTORY_NAME"
PLANNER_FILE="${CONF_DIR}/${DIRECTORY_NAME}/${PLANNER_CONF_FILE}"
log "PLANNER_FILE: $PLANNER_FILE"
PW_FILE="${CONF_DIR}/${DIRECTORY_NAME}/${PW_CONF_FILE}"
log "PW_FILE: $PW_FILE"

# Adds a xml config to $RECORDSERVICE_CONF_FILE
add_to_recordservice_conf() {
  FILE=`find $CONF_DIR/$DIRECTORY_NAME -name $RECORDSERVICE_CONF_FILE`
  log "FILE: $FILE"
  CONF_END="</configuration>"
  NEW_PROPERTY="<property><name>$1</name><value>$2</value></property>"
  TMP_FILE=$CONF_DIR/$DIRECTORY_NAMEtmp-con-file
  cat $FILE | sed "s#$CONF_END#$NEW_PROPERTY#g" > $TMP_FILE
  cp $TMP_FILE $FILE
  rm -f $TMP_FILE
  echo $CONF_END >> $FILE
}

CONF_KEY=recordservice_planner_hostports
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
      log "CONF_VALUE: $CONF_VALUE"
      add_to_recordservice_conf $CONF_KEY $CONF_VALUE
    fi
  ;;
  (*)
    log "Don't understand [$CMD]"
  ;;
esac
