#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# Copy zoo.cfg to $ZK_HOME/conf
cp $IMPALA_HOME/fe/src/test/resources/zoo.cfg $ZK_HOME/conf/zoo.cfg

# Restart zookeeper
$ZK_HOME/bin/zkServer.sh restart
