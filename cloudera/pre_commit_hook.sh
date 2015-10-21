#!/bin/bash
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Script for pre commit validation.

set -x

BASE_DIR=${WORKSPACE}/repos
echo "BASE_DIR: ${BASE_DIR}"

pull_latest_repo() {
  GIT_REPO="git://github.mtv.cloudera.com/CDH/${1}.git --branch ${2}"
  echo "GIT_REPO: ${GIT_REPO}"
  REPO_DIR="${BASE_DIR}/${1}"
  echo "REPO_DIR: ${REPO_DIR}"
  if [ -d "${REPO_DIR}" ]; then
    rm -rf ${REPO_DIR}
  fi
  git clone ${GIT_REPO} ${REPO_DIR}
}

# fetch RecordServiceClient
pull_latest_repo RecordServiceClient master
# pull Imapala-lzo
pull_latest_repo Impala-lzo master
# pull hadoop-lzo
pull_latest_repo hadoop-lzo master
# move RecordService to Impala
mv ${BASE_DIR}/RecordService ${BASE_DIR}/Impala

export JENKINS_USE_TOOLCHAIN=true
. ${BASE_DIR}/RecordServiceClient/jenkins/run_local_tests.sh
