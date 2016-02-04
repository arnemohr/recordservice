// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_UTIL_RECORD_SERVICE_METRICS_H
#define IMPALA_UTIL_RECORD_SERVICE_METRICS_H

#include "util/metrics.h"
#include "util/collection-metrics.h"

namespace impala {

// Contains the keys (strings) for RecordService metrics.
class RecordServiceMetricKeys {
 public:
  // True if the daemon is running this service.
  static const char* RUNNING_PLANNER;
  static const char* RUNNING_WORKER;

  // Number of open Planner/Worker sessions
  static const char* NUM_OPEN_PLANNER_SESSIONS;
  static const char* NUM_OPEN_WORKER_SESSIONS;

  // Number of/number of failed plan requests received by this server.
  static const char* NUM_PLAN_REQUESTS;
  static const char* NUM_FAILED_PLAN_REQUESTS;

  // Number of/number of failed get Schema requests.
  static const char* NUM_GET_SCHEMA_REQUESTS;
  static const char* NUM_FAILED_GET_SCHEMA_REQUESTS;

  // Number of/number of failed ExecTask requests called.
  static const char* NUM_TASK_REQUESTS;
  static const char* NUM_FAILED_TASK_REQUESTS;

  // Number of/number of failed GetTaskStatus requests called.
  static const char* NUM_GET_TASK_STATUS_REQUESTS;
  static const char* NUM_FAILED_GET_TASK_STATUS_REQUESTS;

  // Number of/number of failed Fetch requests called.
  static const char* NUM_FETCH_REQUESTS;
  static const char* NUM_FAILED_FETCH_REQUESTS;

  // Number of tasks that were closed.
  static const char* NUM_CLOSED_TASKS;

  // Total rows fetched.
  static const char* NUM_ROWS_FETCHED;

  // Task size.
  static const char* RECORDSERVICE_TASK_SIZE_KEY;

  // Fetch size (number of rows fetched in one batch).
  static const char* RECORDSERVICE_FETCH_SIZE_KEY;
};

// Global recordservice-wide metrics.
class RecordServiceMetrics {
 public:
  // Properties
  static BooleanProperty* RUNNING_PLANNER;
  static BooleanProperty* RUNNING_WORKER;

  // Gauges
  static IntGauge* NUM_OPEN_PLANNER_SESSIONS;
  static IntGauge* NUM_OPEN_WORKER_SESSIONS;

  // Counters
  static IntCounter* NUM_PLAN_REQUESTS;
  static IntCounter* NUM_FAILED_PLAN_REQUESTS;
  static IntCounter* NUM_GET_SCHEMA_REQUESTS;
  static IntCounter* NUM_FAILED_GET_SCHEMA_REQUESTS;
  static IntCounter* NUM_TASK_REQUESTS;
  static IntCounter* NUM_FAILED_TASK_REQUESTS;
  static IntCounter* NUM_GET_TASK_STATUS_REQUESTS;
  static IntCounter* NUM_FAILED_GET_TASK_STATUS_REQUESTS;
  static IntCounter* NUM_FETCH_REQUESTS;
  static IntCounter* NUM_FAILED_FETCH_REQUESTS;
  static IntCounter* NUM_CLOSED_TASKS;

  static IntCounter* NUM_ROWS_FETCHED;

  // Other
  static StatsMetric<int>* RECORDSERVICE_TASK_SIZE;
  static StatsMetric<int>* RECORDSERVICE_FETCH_SIZE;

  // Creates and initializes all metrics above in 'm'.
  static void CreateMetrics(MetricGroup* m);
};

};

#endif
