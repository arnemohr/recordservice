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

#include "exec/record-service-scan-node.h"

#include <exception>
#include <sstream>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <gutil/strings/substitute.h>

#include "exprs/expr.h"
#include "rpc/thrift-util.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "service/impala-server.h"
#include "util/codec.h"

#include "common/names.h"

using namespace impala;
using namespace llvm;
using namespace strings;

DEFINE_string(recordservice_planner_client_host, "localhost",
    "Host of running RecordService planner.");
DEFINE_int32(recordservice_planner_client_port, 12050,
    "Port of running RecordService planner.");
DEFINE_int32(recordservice_worker_client_port, 13050,
    "Port of running RecordService worker");

namespace impala {
  // Minimal implementation of < for set lookup.
  bool THdfsFileSplit::operator<(const THdfsFileSplit& o) const {
    if (file_name < o.file_name) return true;
    if (file_name > o.file_name) return false;
    if (offset < o.offset) return true;
    if (offset > o.offset) return false;
    return false;
  }
}

RecordServiceScanNode::RecordServiceScanNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ScanNode(pool, tnode, descs),
    tuple_id_(tnode.hdfs_scan_node.tuple_id),
    lock_("RecordServiceScanNode"),
    done_(false),
    // TODO: this needs to be more complex to stop scanner threads when this
    // queue is full.
    num_active_scanners_(0) {
}

RecordServiceScanNode::~RecordServiceScanNode() {
}

Status RecordServiceScanNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ScanNode::Prepare(state));
  state_ = state;
  state_->lock_tracker()->RegisterLock(&lock_);

  materialized_row_batches_.reset(new RowBatchQueue(state, 10));

  DCHECK(scan_range_params_ != NULL)
      << "Must call SetScanRanges() before calling Prepare()";

  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  DCHECK(tuple_desc_ != NULL);
  tuple_byte_size_ = tuple_desc_->byte_size();

  DCHECK(tuple_desc_->table_desc() != NULL);
  hdfs_table_ = static_cast<const HdfsTableDescriptor*>(tuple_desc_->table_desc());
  const vector<SlotDescriptor*>& slots = tuple_desc_->slots();
  for (size_t i = 0; i < slots.size(); ++i) {
    if (!slots[i]->is_materialized()) continue;
    int col_idx = slots[i]->col_pos();
    materialized_slots_.push_back(slots[i]);
    materialized_col_names_.push_back(hdfs_table_->col_descs()[col_idx].name());
  }

  stringstream stmt;
  stmt << "SELECT ";
  if (materialized_slots_.size() == 0) {
    stmt << "count(*)";
  } else {
    for (size_t i = 0; i < materialized_col_names_.size(); ++i) {
      if (i != 0) stmt << ", ";
      stmt << materialized_col_names_[i];
    }
  }

  stmt << " FROM " << hdfs_table_->database() << "." << hdfs_table_->name();

  ThriftClient<recordservice::RecordServicePlannerClient> planner(
      FLAGS_recordservice_planner_client_host, FLAGS_recordservice_planner_client_port,
      ImpalaServer::RECORD_SERVICE_PLANNER_SERVER_NAME,
      AuthManager::GetInstance()->GetExternalAuthProvider());
  RETURN_IF_ERROR(planner.Open());

  // Call the RecordServicePlanner to plan the request and the filter out the
  // tasks that were not assigned.
  // TODO: Ideally, the Impala planner would know how to do this.
  recordservice::TPlanRequestParams params;
  params.request_type = recordservice::TRequestType::Sql;
  params.__set_sql_stmt(stmt.str());
  // Impala doesn't benefit from task combining and it only reduces parallelism.
  params.__set_max_tasks(0);
  recordservice::TPlanRequestResult result;

  QUERY_VLOG_FRAGMENT(state->logger()) << "PlanRequest request: " << params.sql_stmt;

  try {
    planner.iface()->PlanRequest(result, params);
  } catch (const recordservice::TRecordServiceException& e) {
    stringstream ss;
    ss << "Query: " << stmt.str() << "\n" << e.message << " " << e.detail;
    return Status(ss.str());
  } catch (const std::exception& e) {
    return Status(e.what());
  }

  set<THdfsFileSplit> assigned_splits;
  for (int i = 0; i < scan_range_params_->size(); ++i) {
    DCHECK((*scan_range_params_)[i].scan_range.__isset.hdfs_file_split);
    const THdfsFileSplit& split = (*scan_range_params_)[i].scan_range.hdfs_file_split;
    assigned_splits.insert(split);
  }

  // Walk through all the tasks (which includes the splits for the entire table) and
  // filter out the ones that aren't assigned to this node.

  scoped_ptr<Codec> decompressor;
  Codec::CreateDecompressor(NULL, false, THdfsCompression::LZ4, &decompressor);

  QUERY_VLOG_FRAGMENT(state->logger()) << "PlanRequest returned "
      << result.tasks.size() << " tasks.";

  TRecordServiceTask rs_task;
  TExecRequest exec_req;
  for (int i = 0; i < result.tasks.size(); ++i) {
    uint32_t size = result.tasks[i].task.size();
    RETURN_IF_ERROR(DeserializeThriftMsg(
        reinterpret_cast<const uint8_t*>(result.tasks[i].task.data()),
        &size, true, &rs_task));

    string decompressed_exec_req;
    RETURN_IF_ERROR(decompressor->Decompress(
        rs_task.request, true, &decompressed_exec_req));

    size = decompressed_exec_req.size();
    RETURN_IF_ERROR(DeserializeThriftMsg(
        reinterpret_cast<const uint8_t*>(decompressed_exec_req.data()),
        &size, true, &exec_req));

    const THdfsFileSplit& split = exec_req.query_exec_request.per_node_scan_ranges.
        begin()->second[0].scan_range.hdfs_file_split;
    if (assigned_splits.find(split) != assigned_splits.end()) {
      tasks_.push_back(result.tasks[i].task);
    }
  }

  task_id_ = 0;
  if (tasks_.size() == 0) {
    done_ = true;
    materialized_row_batches_->Shutdown();
  }

  return Status::OK();
}

Status RecordServiceScanNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));
  if (done_) return Status::OK();

  num_scanner_threads_started_counter_ =
      ADD_COUNTER(runtime_profile(), NUM_SCANNER_THREADS_STARTED, TUnit::UNIT);

  // Reserve one thread token.
  state->resource_pool()->ReserveOptionalTokens(1);
  if (state->query_options().num_scanner_threads > 0) {
    state->resource_pool()->set_max_quota(
        state->query_options().num_scanner_threads);
  }

  state->resource_pool()->SetThreadAvailableCb(
      bind<void>(mem_fn(&RecordServiceScanNode::ThreadTokenAvailableCb), this, _1));
  ThreadTokenAvailableCb(state->resource_pool());
  return Status::OK();
}

Status RecordServiceScanNode::GetNext(RuntimeState* state,
    RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());

 if (ReachedLimit()) {
    *eos = true;
    return Status::OK();
  }

  *eos = false;
  RowBatch* materialized_batch = materialized_row_batches_->GetBatch();
  if (materialized_batch != NULL) {
    row_batch->AcquireState(materialized_batch);
    num_rows_returned_ += row_batch->num_rows();
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);

    if (ReachedLimit()) {
      int num_rows_over = num_rows_returned_ - limit_;
      row_batch->set_num_rows(row_batch->num_rows() - num_rows_over);
      num_rows_returned_ -= num_rows_over;
      COUNTER_SET(rows_returned_counter_, num_rows_returned_);
      *eos = true;
      done_ = true;
      materialized_row_batches_->Shutdown();
    }
    delete materialized_batch;
  } else {
    *eos = true;
  }

  Status status;
  {
    unique_lock<Lock> l(lock_);
    status = status_;
  }
  if (status.ok()) LOG_ROW_BATCH_ROWS(row_batch, state->logger());
  return status;
}

void RecordServiceScanNode::ThreadTokenAvailableCb(
    ThreadResourceMgr::ResourcePool* pool) {
  while (true) {
    unique_lock<Lock> lock(lock_);
    if (done_ || task_id_ >= tasks_.size()) {
      // We're either done or all tasks have been assigned a thread
      break;
    }

    // Check if we can get a token.
    bool is_reserved_dummy = false;
    if (!pool->TryAcquireThreadToken(&is_reserved_dummy)) break;

    ++num_active_scanners_;
    COUNTER_ADD(num_scanner_threads_started_counter_, 1);

    stringstream ss;
    ss << "scanner-thread(" << num_scanner_threads_started_counter_->value() << ")";
    scanner_threads_.AddThread(
        new Thread("record-service-scan-node", ss.str(),
            &RecordServiceScanNode::ScannerThread, this, task_id_++));
  }
}

void RecordServiceScanNode::ScannerThread(int task_id) {
  SCOPED_THREAD_COUNTER_MEASUREMENT(scanner_thread_counters());
  SCOPED_TIMER(state_->total_cpu_timer());
  DCHECK_LT(task_id, tasks_.size());

  // ExprContexts are not thread safe, so make contexts for each thread.
  vector<ExprContext*> per_thread_conjunct_ctxs;
  Status status = Expr::Clone(conjunct_ctxs_, state_, &per_thread_conjunct_ctxs);

  // Connect to the local RecordService worker. Thrift clients are not thread safe.
  // TODO: pool these.
  ThriftClient<recordservice::RecordServiceWorkerClient> client(
      FLAGS_hostname, FLAGS_recordservice_worker_client_port,
      ImpalaServer::RECORD_SERVICE_WORKER_SERVER_NAME,
      AuthManager::GetInstance()->GetExternalAuthProvider());

  while (true) {
    if (status.ok()) status = client.Open();
    if (status.ok()) status = ProcessTask(&client, task_id, per_thread_conjunct_ctxs);

    // Check status, and grab the next task id.
    unique_lock<Lock> l(lock_);
    // Check for errors.
    if (UNLIKELY(!status.ok())) {
      if (status_.ok()) {
        status_ = status;
        done_ = true;
      }
      goto done;
    }
    DCHECK_GE(num_active_scanners_, 1);

    // Check if we are done.
    if (task_id_ >= tasks_.size()) {
      if (num_active_scanners_ == 1) done_ = true;
      goto done;
    }

    // Check if we still have thread token.
    if (state_->resource_pool()->optional_exceeded()) goto done;

    task_id = task_id_++;
    continue;

done:
    // Lock is still taken
    // Only exit from this function.
    --num_active_scanners_;
    if (done_) materialized_row_batches_->Shutdown();
    Expr::Close(per_thread_conjunct_ctxs, state_);
    break;
  }

  state_->resource_pool()->ReleaseThreadToken(false);
}

struct ScopedTask {
  ScopedTask(ThriftClient<recordservice::RecordServiceWorkerClient>* client,
      const recordservice::TUniqueId& handle)
    : client_(client), handle_(handle) {
  }

  ~ScopedTask() {
    client_->iface()->CloseTask(handle_);
  }

 private:
  ThriftClient<recordservice::RecordServiceWorkerClient>* client_;
  const recordservice::TUniqueId handle_;
};

Status RecordServiceScanNode::ProcessTask(
    ThriftClient<recordservice::RecordServiceWorkerClient>* client, int task_id,
    const vector<ExprContext*>& conjunct_ctxs) {
  recordservice::TExecTaskParams params;
  params.task = tasks_[task_id];
  recordservice::TExecTaskResult result;

  try {
    client->iface()->ExecTask(result, params);
  } catch (const recordservice::TRecordServiceException& e) {
    return Status(e.message.c_str());
  } catch (const std::exception& e) {
    return Status(e.what());
  }

  // Add a scoped cleanup.
  ScopedTask task_cleanup(client, result.handle);

  recordservice::TFetchResult fetch_result;
  recordservice::TFetchParams fetch_params;
  fetch_params.handle = result.handle;

  // keep fetching batches
  while (!done_) {
    try {
      client->iface()->Fetch(fetch_result, fetch_params);
      if (!fetch_result.__isset.columnar_records) {
        return Status("Expecting RecordService to return columnar row batches.");
      }
    } catch (const recordservice::TRecordServiceException& e) {
      return Status(e.message.c_str());
    } catch (const std::exception& e) {
      return Status(e.what());
    }

    // Convert into row batch.
    const recordservice::TColumnarRecords& input_batch = fetch_result.columnar_records;

    // TODO: validate schema.
    if (materialized_slots_.size() == 0) {
      if (input_batch.cols.size() != 1) {
        return Status("Expecting count(*) to return 1 column.");
      }
      if (input_batch.cols[0].data.size() != sizeof(int64_t)) {
        return Status("Expecting count(*) to return a BIGINT.");
      }
      const char* data = input_batch.cols[0].data.data();
      int64_t count = *reinterpret_cast<const int64_t*>(data);
      while (count > 0) {
        int num_rows = min(count, static_cast<int64_t>(state_->batch_size()));
        RowBatch* batch = new RowBatch(row_desc(), num_rows, mem_tracker());
        batch->AddRows(num_rows);
        batch->CommitRows(num_rows);
        materialized_row_batches_->AddBatch(batch);
        count -= num_rows;
      }
      break;
    } else {
      if (input_batch.cols.size() != materialized_slots_.size()) {
        stringstream ss;
        ss << "Invalid row batch from RecordService. Expecting "
          << materialized_slots_.size()
          << " cols. RecordService returned " << input_batch.cols.size() << " cols.";
        return Status(ss.str());
      }

      if (fetch_result.num_records == 0) {
        DCHECK(fetch_result.done);
        break;
      }
    }

    std::auto_ptr<RowBatch> row_batch(
        new RowBatch(row_desc(), fetch_result.num_records, mem_tracker()));

    Tuple* tuple = Tuple::Create(row_batch->MaxTupleBufferSize(),
        row_batch->tuple_data_pool());

    // TODO: this really needs codegen/optimizations
    vector<const char*> data_values;
    for (int i = 0; i < input_batch.cols.size(); ++i) {
      data_values.push_back(input_batch.cols[i].data.data());
      COUNTER_ADD(bytes_read_counter_, input_batch.cols[i].data.size());
      COUNTER_ADD(bytes_read_counter_, input_batch.cols[i].is_null.size());
    }

    COUNTER_ADD(rows_read_counter_, fetch_result.num_records);
    SCOPED_TIMER(materialize_tuple_timer_);
    for (int i = 0; i < fetch_result.num_records; ++i) {
      TupleRow* row = row_batch->GetRow(row_batch->AddRow());
      row->SetTuple(0, tuple);

      for (int c = 0; c < materialized_slots_.size(); ++c) {
        const recordservice::TColumnData& data = input_batch.cols[c];
        if (data.is_null[i]) {
          tuple->SetNull(materialized_slots_[c]->null_indicator_offset());
          continue;
        }

        tuple->SetNotNull(materialized_slots_[c]->null_indicator_offset());
        void* slot = tuple->GetSlot(materialized_slots_[c]->tuple_offset());
        switch (materialized_slots_[c]->type().type) {
          case TYPE_BOOLEAN:
          case TYPE_TINYINT:
          case TYPE_SMALLINT:
          case TYPE_INT:
          case TYPE_BIGINT:
          case TYPE_FLOAT:
          case TYPE_DOUBLE:
          case TYPE_DECIMAL:
            memcpy(slot, data_values[c], materialized_slots_[c]->type().GetByteSize());
            data_values[c] += materialized_slots_[c]->type().GetByteSize();
            break;

          case TYPE_TIMESTAMP: {
            int64_t millis = *reinterpret_cast<const int64_t*>(data_values[c]);
            data_values[c] += sizeof(int64_t);
            int32_t nanos = *reinterpret_cast<const int32_t*>(data_values[c]);
            data_values[c] += sizeof(int32_t);
            reinterpret_cast<TimestampValue*>(slot)->FromMillisAndNanos(millis, nanos);
            break;
          }

          case TYPE_CHAR: {
            if (materialized_slots_[c]->type().GetByteSize() != 0) {
              memcpy(slot, data_values[c], materialized_slots_[c]->type().len);
              data_values[c] += materialized_slots_[c]->type().GetByteSize();
            } else {
              // CHAR is too long and not inlined. Treat it like STRING.
              StringValue* sv = reinterpret_cast<StringValue*>(slot);
              sv->len = materialized_slots_[c]->type().len;
              sv->ptr = reinterpret_cast<char*>(
                  row_batch->tuple_data_pool()->Allocate(sv->len));
              memcpy(sv->ptr, data_values[c], sv->len);
              data_values[c] += sv->len;
            }
            break;
          }

          case TYPE_STRING:
          case TYPE_VARCHAR: {
            // TODO: this copy can be removed by having the row batch take ownership
            // of the string data from the TParquetRowBatch.
            StringValue* sv = reinterpret_cast<StringValue*>(slot);
            sv->len = *reinterpret_cast<const int32_t*>(data_values[c]);
            data_values[c] += sizeof(int32_t);
            sv->ptr = reinterpret_cast<char*>(
                row_batch->tuple_data_pool()->Allocate(sv->len));
            memcpy(sv->ptr, data_values[c], sv->len);
            data_values[c] += sv->len;
            break;
          }

          default:
            CHECK(false) << "Not implemented";
        }
      }

      if (EvalConjuncts(&conjunct_ctxs[0], conjunct_ctxs.size(), row)) {
        row_batch->CommitLastRow();
        tuple = next_tuple(tuple);
        if (ReachedLimit()) break;
      }
    }

    if (row_batch->num_rows() != 0) {
      materialized_row_batches_->AddBatch(row_batch.release());
    }

    if (fetch_result.done) break;
  }

  return Status::OK();
}

void RecordServiceScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (!done_) {
    unique_lock<Lock> l(lock_);
    done_ = true;
    materialized_row_batches_->Shutdown();
  }

  scanner_threads_.JoinAll();
  DCHECK_EQ(num_active_scanners_, 0);

  materialized_row_batches_->Cleanup();
  state->resource_pool()->SetThreadAvailableCb(NULL);

  ScanNode::Close(state);
}
