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

#include "util/recordservice-metrics.h"

#include "util/debug-util.h"

#include "common/names.h"

namespace impala {

#define DEFINE_INT_COUNTER(NAME, STRING)\
  const char* RecordServiceMetricKeys::NAME = STRING;\
  IntCounter* RecordServiceMetrics::NAME;

#define DEFINE_INT_GAUGE(NAME, STRING)\
  const char* RecordServiceMetricKeys::NAME = STRING;\
  IntGauge* RecordServiceMetrics::NAME;

#define ADD_INT_COUNTER(m, NAME, desc)\
  NAME = m->AddCounter(RecordServiceMetricKeys::NAME, 0L, TUnit::UNIT, desc);

#define ADD_INT_GAUGE(m, NAME, desc)\
  NAME = m->AddGauge<int64_t>(RecordServiceMetricKeys::NAME, 0L, TUnit::UNIT, desc);

DEFINE_INT_GAUGE(NUM_OPEN_PLANNER_SESSIONS,
    "record-service.num-open-planner-sessions");
DEFINE_INT_GAUGE(NUM_OPEN_WORKER_SESSIONS,
    "record-service.num-open-worker-sessions");

DEFINE_INT_COUNTER(NUM_PLAN_REQUESTS,
    "record-service.num-plan-requests");
DEFINE_INT_COUNTER(NUM_FAILED_PLAN_REQUESTS,
    "record-service.num-failed-plan-requests");
DEFINE_INT_COUNTER(NUM_GET_SCHEMA_REQUESTS,
    "record-service.num-get-plan-requests");
DEFINE_INT_COUNTER(NUM_FAILED_GET_SCHEMA_REQUESTS,
    "record-service.num-failed-get-plan-requests");
DEFINE_INT_COUNTER(NUM_TASK_REQUESTS,
    "record-service.num-task-requests");
DEFINE_INT_COUNTER(NUM_FAILED_TASK_REQUESTS,
    "record-service.num-failed-task-requests");
DEFINE_INT_COUNTER(NUM_GET_TASK_STATUS_REQUESTS,
    "record-service.num-get-task-status-requests");
DEFINE_INT_COUNTER(NUM_FAILED_GET_TASK_STATUS_REQUESTS,
    "record-service.num-failed-get-task-status-requests");
DEFINE_INT_COUNTER(NUM_FETCH_REQUESTS,
    "record-service.num-fetch-requests");
DEFINE_INT_COUNTER(NUM_FAILED_FETCH_REQUESTS,
    "record-service.num-failed-fetch-requests");
DEFINE_INT_COUNTER(NUM_CLOSED_TASKS,
    "record-service.num-closed-tasks");

DEFINE_INT_COUNTER(NUM_ROWS_FETCHED,
    "record-service.num-rows-fetched");

const char* RecordServiceMetricKeys::RUNNING_PLANNER =
    "record-service.running-planner";
const char* RecordServiceMetricKeys::RUNNING_WORKER =
    "record-service.running-worker";
const char* RecordServiceMetricKeys::RECORDSERVICE_TASK_SIZE_KEY =
    "record-service.task-bytes";

BooleanProperty* RecordServiceMetrics::RUNNING_PLANNER = NULL;
BooleanProperty* RecordServiceMetrics::RUNNING_WORKER = NULL;

// Other
StatsMetric<int>* RecordServiceMetrics::RECORDSERVICE_TASK_SIZE = NULL;

void RecordServiceMetrics::CreateMetrics(MetricGroup* m) {
  RUNNING_PLANNER = m->AddPropertyWithDesc<bool>(
      RecordServiceMetricKeys::RUNNING_PLANNER, false,
      "If true this is running the RecordServicePlanner thrift service.");
  RUNNING_WORKER = m->AddPropertyWithDesc<bool>(
      RecordServiceMetricKeys::RUNNING_WORKER, false,
      "If true this is running the RecordServiceWorker thrift service.");

  ADD_INT_GAUGE(m, NUM_OPEN_PLANNER_SESSIONS,
      "Number of connected planner clients.");
  ADD_INT_GAUGE(m, NUM_OPEN_WORKER_SESSIONS,
      "Number of connected worker clients.");

  ADD_INT_COUNTER(m, NUM_PLAN_REQUESTS,
      "Number of Plan requests received by the service.");
  ADD_INT_COUNTER(m, NUM_FAILED_PLAN_REQUESTS,
      "Number of Plan requests that failed.");
  ADD_INT_COUNTER(m, NUM_GET_SCHEMA_REQUESTS,
      "Number of GetSchema requests received by the service.");
  ADD_INT_COUNTER(m, NUM_FAILED_GET_SCHEMA_REQUESTS,
      "Number of GetSchema requests that failed.");
  ADD_INT_COUNTER(m, NUM_TASK_REQUESTS,
      "Number of ExecTask requests received by the service.");
  ADD_INT_COUNTER(m, NUM_FAILED_TASK_REQUESTS,
      "Number of ExecTask requests that failed.");
  ADD_INT_COUNTER(m, NUM_GET_TASK_STATUS_REQUESTS,
      "Number of GetTaskStatus requests received by the service.");
  ADD_INT_COUNTER(m, NUM_FAILED_GET_TASK_STATUS_REQUESTS,
      "Number of GetTaskStatus requests that failed.");
  ADD_INT_COUNTER(m, NUM_FETCH_REQUESTS,
      "Number of Fetch requests received by the service.");
  ADD_INT_COUNTER(m, NUM_FAILED_FETCH_REQUESTS,
      "Number of Fetch requests that failed.");
  ADD_INT_COUNTER(m, NUM_CLOSED_TASKS,
      "Number of CloseTask requests.");

  ADD_INT_COUNTER(m, NUM_ROWS_FETCHED,
      "Number of rows fetched across all tasks.");

  RECORDSERVICE_TASK_SIZE = StatsMetric<int>::CreateAndRegister(
      m, RecordServiceMetricKeys::RECORDSERVICE_TASK_SIZE_KEY);
}

}
