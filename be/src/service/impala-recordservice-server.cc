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

#include "service/impala-server.h"
#include "service/impala-server.inline.h"

#include <algorithm>
#include <boost/algorithm/string/join.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/unordered_set.hpp>
#include <thrift/protocol/TDebugProtocol.h>
#include <boost/foreach.hpp>
#include <boost/algorithm/string.hpp>
#include <functional>
#include <gutil/strings/substitute.h>
#include <openssl/hmac.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>

#include "common/logging.h"
#include "common/version.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "exprs/slot-ref.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/raw-value.h"
#include "service/query-exec-state.h"
#include "service/query-options.h"
#include "rpc/thrift-util.h"
#include "util/codec.h"
#include "util/debug-util.h"
#include "util/hdfs-util.h"
#include "util/recordservice-metrics.h"
#include "service/hs2-util.h"

#include "common/names.h"

using namespace strings;
using namespace beeswax; // Converting QueryState
using namespace apache::thrift;

DECLARE_int32(recordservice_worker_port);

// Maximum tasks to generate per plan request.
// -1: use server default.
//  0: don't combine tasks.
DEFINE_int32(max_tasks, -1,
    "The default maximum number of tasks to generate per PlanRequest(). If set to < 0, "
    "the server will pick a default based on cluster membership and machine specs. If "
    "set to 0, no tasks will be combined.");

// Max task size in bytes.
DEFINE_int64(max_task_size, 2 * 1024L * 1024L * 1024L,
    "Max task size in bytes. Only used if combining tasks.");

// Number of tasks to generate per core (per host). There is a trade off. The larger
// the value, the more tasks we will generate per PlanRequest(). This increases
// parallelism and fault tolerance granularity. The downside is descreased efficiency
// (more task start up time, less scheduling opportunities when executing a task etc.).
// This also should vary depending on the task launch overhead.
DEFINE_int32(tasks_per_core, 10, "Number of tasks to generate per core.");

// This value has a big impact on performance. For simple queries (1 bigint col),
// 5000 is a 2x improvement over a fetch size of 1024.
// TODO: investigate more
static const int DEFAULT_FETCH_SIZE = 5000;

// Names of temporary tables used to service path based requests.
// FIXME: everything about temp tables is a hack.
static const char* TEMP_DB = "rs_tmp_db";
static const char* TEMP_TBL = "tmp_tbl";

// Byte size of hadoop file headers.
// TODO: best place for these files?
const int HADOOP_FILE_HEADER_SIZE = 3;

const uint8_t AVRO_HEADER[HADOOP_FILE_HEADER_SIZE] = { 'O', 'b', 'j' };
const uint8_t PARQUET_HEADER[HADOOP_FILE_HEADER_SIZE] = { 'P', 'A', 'R' };
const uint8_t SEQUENCE_HEADER[HADOOP_FILE_HEADER_SIZE] = { 'S', 'E', 'Q' };
const uint8_t RCFILE_HEADER[HADOOP_FILE_HEADER_SIZE] = { 'R', 'C', 'F' };

namespace impala {

void ImpalaServer::ThrowRecordServiceException(
    const recordservice::TErrorCode::type& code,
    const string& msg, const string& detail) {
  LOG(INFO) << "RecordService request failed. code=" << code
            << " msg=" << msg << " detail=" << detail;
  recordservice::TRecordServiceException ex;
  ex.code = code;
  ex.message = msg;
  if (!detail.empty()) ex.__set_detail(detail);
  throw ex;
}

inline void ThrowFetchException(const Status& status) {
  DCHECK(!status.ok());
  recordservice::TRecordServiceException ex;
  if (status.IsCancelled()) {
    ImpalaServer::ThrowRecordServiceException(recordservice::TErrorCode::CANCELLED,
        "Task failed because it was cancelled.",
        status.msg().GetFullMessageDetails());
  } else if (status.IsMemLimitExceeded()) {
    ImpalaServer::ThrowRecordServiceException(recordservice::TErrorCode::OUT_OF_MEMORY,
        "Task failed because it ran out of memory.",
        status.msg().GetFullMessageDetails());
    // FIXME: make sure this contains the mem tracker dump.
  } else {
    ImpalaServer::ThrowRecordServiceException(recordservice::TErrorCode::INTERNAL_ERROR,
        "Task failed due to an internal error.",
        status.msg().GetFullMessageDetails());
  }
}

// Base class for test result set serializations. The functions in here and
// not used in the RecordService path.
//
// Used to abstract away serializing results. The calling pattern is:
//
// BaseResult* result = new ...
// result->Init();
// for each rpc:
//   result->SetReturnBuffer();
//   for each batch:
//     result->AddBatch()
//   result->FinalizeResult()
class ImpalaServer::BaseResultSet : public ImpalaServer::QueryResultSet {
 public:
  virtual Status AddOneRow(const TResultRow& row) {
    CHECK(false) << "Not used";
    return Status::OK();
  }

  virtual int AddRows(const QueryResultSet* other, int start_idx, int num_rows) {
    CHECK(false) << "Not used";
    return num_rows;
  }

  virtual int64_t ByteSize(int start_idx, int num_rows) {
    CHECK(false) << "Not used";
    return sizeof(int64_t);
  }

  virtual Status AddOneRow(const vector<void*>& col_values, const vector<int>& scales) {
    CHECK(false) << "Not used";
    return Status::OK();
  }

  virtual void Init(const TResultSetMetadata& md, int fetch_size) {
    for (int i = 0; i < md.columns.size(); ++i) {
      types_.push_back(ColumnType::FromThrift(md.columns[i].columnType));
      if (types_[i] == TYPE_TIMESTAMP) {
        type_sizes_.push_back(12);
      } else {
        type_sizes_.push_back(types_[i].GetByteSize());
      }
    }
  }

  virtual void FinalizeResult() {}

  virtual bool supports_batch_add() const { return true; }

  // This should be set for every fetch request so that the results are directly
  // populated in the thrift result object (to avoid a copy).
  virtual void SetReturnBuffer(recordservice::TFetchResult* result) = 0;

  virtual size_t size() { return result_ == NULL ? 0 : result_->num_records; }

 protected:
  BaseResultSet() : result_(NULL) {}
  recordservice::TFetchResult* result_;

  vector<ColumnType> types_;
  vector<int> type_sizes_;
};

// Additional state for the RecordService. Put here instead of QueryExecState
// to separate from Impala code.
class ImpalaServer::RecordServiceTaskState {
 public:
  RecordServiceTaskState() : offset(0), counters_initialized(false) {}

  // Maximum number of rows to return per fetch. Impala's batch size is set to
  // this value.
  int fetch_size;

  // The offset to return records.
  int64_t offset;

  recordservice::TRecordFormat::type format;
  scoped_ptr<ImpalaServer::BaseResultSet> results;

  // Populated on first call to Fetch(). At that point the query has for sure
  // made enough progress that the counters are initialized.
  bool counters_initialized;
  RuntimeProfile::Counter* serialize_timer;
  RuntimeProfile::Counter* client_timer;

  RuntimeProfile::Counter* bytes_assigned_counter;
  RuntimeProfile::Counter* bytes_read_counter;
  RuntimeProfile::Counter* bytes_read_local_counter;
  RuntimeProfile::Counter* rows_read_counter;
  RuntimeProfile::Counter* rows_returned_counter;
  RuntimeProfile::Counter* decompression_timer;
  RuntimeProfile::Counter* hdfs_throughput_counter;
};

// This is the parquet plain encoding, meaning we append the little endian version
// of the value to the end of the buffer.
class ImpalaServer::RecordServiceParquetResultSet : public ImpalaServer::BaseResultSet {
 public:
  RecordServiceParquetResultSet(RecordServiceTaskState* state, bool all_slot_refs,
      const vector<ExprContext*>& output_exprs)
    : state_(state),
      all_slot_refs_(all_slot_refs) {
    if (all_slot_refs) {
      slot_descs_.resize(output_exprs.size());
      for (int i = 0; i < output_exprs.size(); ++i) {
        SlotRef* slot_ref = reinterpret_cast<SlotRef*>(output_exprs[i]->root());
        slot_descs_[i].byte_offset = slot_ref->slot_offset();
        slot_descs_[i].null_offset = slot_ref->null_indicator();
      }
    }
  }

  virtual void SetReturnBuffer(recordservice::TFetchResult* result) {
    result_ = result;
    result_->__isset.columnar_records = true;
    result_->columnar_records.cols.resize(types_.size());
  }

  virtual void AddRowBatch(RowBatch* input, int row_idx, int num_rows,
      vector<ExprContext*>* ctxs) {
    DCHECK(result_->__isset.columnar_records);
    recordservice::TColumnarRecords& batch = result_->columnar_records;

    if (state_->offset > 0) {
      // There is an offset, skip offset num rows.
      if (num_rows <= state_->offset) {
        state_->offset -= num_rows;
        return;
      } else {
        row_idx += state_->offset;
        num_rows -= state_->offset;
        state_->offset = 0;
      }
    }

    if (all_slot_refs_) {
      // In this case, all the output exprs are slot refs and we want to serialize them
      // to the RecordService format. To do this we:
      // 1. Reserve the outgoing buffer to the max size (for fixed length types).
      // 2. Append the current value to the outgoing buffer.
      // 3. Resize the outgoing buffer when we are done with the row batch (which
      // can be sparse due to NULLs).
      DCHECK_EQ(ctxs->size(), slot_descs_.size());
      const int num_cols = slot_descs_.size();

      // Reserve the size of the output where possible.
      for (int c = 0; c < num_cols; ++c) {
        DCHECK_EQ(batch.cols[c].is_null.size(), 0);
        DCHECK_EQ(batch.cols[c].data.size(), 0);

        batch.cols[c].is_null.resize(num_rows);
        if (type_sizes_[c] != 0) {
          batch.cols[c].data.resize(num_rows * type_sizes_[c]);
        }
      }

      // Only used for fixed length types. data[c] is the ptr that the next value
      // should be appended at.
      char* data[num_cols];
      for (int c = 0; c < num_cols; ++c) {
        data[c] = (char*)batch.cols[c].data.data();
      }

      // This loop is extremely perf sensitive.
      for (int i = 0; i < num_rows; ++i) {
        Tuple* tuple = input->GetRow(row_idx++)->GetTuple(0);
        for (int c = 0; c < num_cols; ++c) {
          bool is_null = tuple->IsNull(slot_descs_[c].null_offset);
          batch.cols[c].is_null[i] = is_null;
          if (is_null) continue;

          const int type_size = type_sizes_[c];
          if (type_size == 0) {
            // TODO: this resizing can't be good. The rowbatch should keep track of
            // how long the string data is.
            string& dst = batch.cols[c].data;
            int offset = dst.size();
            const StringValue* sv = tuple->GetStringSlot(slot_descs_[c].byte_offset);
            if (types_[c].type == TYPE_CHAR) {
              // CHAR(N) is too long and not inlined. Impala treats it as a StringValue
              // but we don't want to include the constant length in the serialized
              // result.
              int len = types_[c].len;
              dst.resize(offset + len);
              memcpy((char*)dst.data() + offset, sv->ptr, len);
            } else {
              int len = sv->len + sizeof(int32_t);
              dst.resize(offset + len);
              memcpy((char*)dst.data() + offset, &sv->len, sizeof(int32_t));
              memcpy((char*)dst.data() + offset + sizeof(int32_t), sv->ptr, sv->len);
            }
          } else {
            const void* slot = tuple->GetSlot(slot_descs_[c].byte_offset);
            if (types_[c] == TYPE_TIMESTAMP) {
              DCHECK_EQ(type_size, 12);
              const TimestampValue* ts = reinterpret_cast<const TimestampValue*>(slot);
              int64_t millis;
              int32_t nanos;
              ts->ToMillisAndNanos(&millis, &nanos);
              memcpy(data[c], &millis, sizeof(int64_t));
              memcpy(data[c] + sizeof(int64_t), &nanos, sizeof(int32_t));
            } else {
              memcpy(data[c], slot, type_size);
            }
            data[c] += type_size;
          }
        }
      }

      // For fixed-length columns, shrink the size if necessary. In the case of NULLs,
      // we could have resized the buffer bigger than necessary.
      for (int c = 0; c < num_cols; ++c) {
        if (type_sizes_[c] == 0) continue;
        int size = data[c] - batch.cols[c].data.data();
        if (batch.cols[c].data.size() != size) {
          batch.cols[c].data.resize(size);
        }
      }
    } else {
      // Reserve the size of the output where possible.
      for (int c = 0; c < ctxs->size(); ++c) {
        DCHECK_EQ(batch.cols[c].is_null.size(), 0);
        DCHECK_EQ(batch.cols[c].data.size(), 0);

        batch.cols[c].is_null.reserve(num_rows);
        if (type_sizes_[c] != 0) {
          batch.cols[c].data.reserve(num_rows * type_sizes_[c]);
        }
      }
      for (int i = 0; i < num_rows; ++i) {
        TupleRow* row = input->GetRow(row_idx++);

        for (int c = 0; c < ctxs->size(); ++c) {
          const void* v = (*ctxs)[c]->GetValue(row);
          batch.cols[c].is_null.push_back(v == NULL);
          if (v == NULL) continue;

          string& data = batch.cols[c].data;
          int offset = data.size();

          // Encode the values here. For non-string types, just write the value as
          // little endian. For strings, it is the length(little endian) followed
          // by the string.
          const int type_size = type_sizes_[c];
          if (type_size == 0) {
            const StringValue* sv = reinterpret_cast<const StringValue*>(v);
            if (types_[c].type == TYPE_CHAR) {
              // CHAR(N) is too long and not inlined. Impala treats it as a StringValue
              // but we don't want to include the constant length in the serialized
              // result.
              int len = types_[c].len;
              data.resize(offset + len);
              memcpy((char*)data.data() + offset, sv->ptr, len);
            } else {
              int len = sv->len + sizeof(int32_t);
              data.resize(offset + len);
              memcpy((char*)data.data() + offset, &sv->len, sizeof(int32_t));
              memcpy((char*)data.data() + offset + sizeof(int32_t), sv->ptr, sv->len);
            }
          } else {
            data.resize(offset + type_size);
            memcpy((char*)data.data() + offset, v, type_size);
          }
        }
      }
    }
    result_->num_records += num_rows;
  }

 private:
  struct SlotDesc {
    // Byte offset in tuple
    int byte_offset;
    NullIndicatorOffset null_offset;

    SlotDesc() : null_offset(0, 0) {}
  };

  // Unowned.
  RecordServiceTaskState* state_;

  // If true, all the output exprs are slot refs.
  bool all_slot_refs_;

  // Cache of the slot desc. Only set if all_slot_refs_ is true. We'll use this
  // instead of the exprs (for performance).
  vector<SlotDesc> slot_descs_;
};

void ImpalaServer::GetRecordServiceSession(ScopedSessionState* session) {
  Status status = session->WithSession(ThriftServer::GetThreadConnectionId());
  if (!status.ok()) {
    // The session is tied to the thrift connection so the only way this can
    // happen is if the server timed out the session.
    ThrowRecordServiceException(recordservice::TErrorCode::CONNECTION_TIMED_OUT,
        "Connection has timed out. Reconnect to the server.");
  }
}

recordservice::TType ToRecordServiceType(const ColumnType& t) {
  recordservice::TType result;
  switch (t.type) {
    case TYPE_BOOLEAN:
      result.type_id = recordservice::TTypeId::BOOLEAN;
      break;
    case TYPE_TINYINT:
      result.type_id = recordservice::TTypeId::TINYINT;
      break;
    case TYPE_SMALLINT:
      result.type_id = recordservice::TTypeId::SMALLINT;
      break;
    case TYPE_INT:
      result.type_id = recordservice::TTypeId::INT;
      break;
    case TYPE_BIGINT:
      result.type_id = recordservice::TTypeId::BIGINT;
      break;
    case TYPE_FLOAT:
      result.type_id = recordservice::TTypeId::FLOAT;
      break;
    case TYPE_DOUBLE:
      result.type_id = recordservice::TTypeId::DOUBLE;
      break;
    case TYPE_STRING:
      result.type_id = recordservice::TTypeId::STRING;
      break;
    case TYPE_VARCHAR:
      result.type_id = recordservice::TTypeId::VARCHAR;
      result.__set_len(t.len);
      break;
    case TYPE_CHAR:
      result.type_id = recordservice::TTypeId::CHAR;
      result.__set_len(t.len);
      break;
    case TYPE_TIMESTAMP:
      result.type_id = recordservice::TTypeId::TIMESTAMP_NANOS;
      break;
    case TYPE_DECIMAL:
      result.type_id = recordservice::TTypeId::DECIMAL;
      result.__set_precision(t.precision);
      result.__set_scale(t.scale);
      break;
    default:
      ImpalaServer::ThrowRecordServiceException(
          recordservice::TErrorCode::INVALID_REQUEST, "Not supported type.");
  }
  return result;
}

// Converts a schema to the SQL create table schema. e.g. schema to
// (col1Name col1Type, col2Name col2Type, ...)
Status SchemaToSqlString(const recordservice::TSchema& schema, string* sql) {
  stringstream ss;
  ss << "(";
  for (int i = 0; i < schema.cols.size(); ++i) {
    if (i != 0) ss << ", ";
    ss << schema.cols[i].name << " ";
    switch (schema.cols[i].type.type_id) {
      case recordservice::TTypeId::BOOLEAN:
        ss << "BOOLEAN";
        break;
      case recordservice::TTypeId::TINYINT:
        ss << "TINYINT";
        break;
      case recordservice::TTypeId::SMALLINT:
        ss << "SMALLINT";
        break;
      case recordservice::TTypeId::INT:
        ss << "INT";
        break;
      case recordservice::TTypeId::BIGINT:
        ss << "BIGINT";
        break;
      case recordservice::TTypeId::FLOAT:
        ss << "FLOAT";
        break;
      case recordservice::TTypeId::DOUBLE:
        ss << "DOUBLE";
        break;
      case recordservice::TTypeId::STRING:
        ss << "STRING";
        break;
      case recordservice::TTypeId::CHAR:
      case recordservice::TTypeId::VARCHAR:
        if (!schema.cols[i].type.__isset.len) {
          stringstream err;
          err << "Invalid schema. 'len' must be set for CHAR/VARCHAR type. Type="
              << apache::thrift::ThriftDebugString(schema.cols[i].type);
          return Status(err.str());
        }
        if (schema.cols[i].type.type_id == recordservice::TTypeId::CHAR) {
          ss << "CHAR";
        } else {
          ss << "VARCHAR";
        }
        ss << "(" << schema.cols[i].type.len << ")";
        break;
      case recordservice::TTypeId::DECIMAL:
        if (!schema.cols[i].type.__isset.precision ||
            !schema.cols[i].type.__isset.scale) {
          stringstream err;
          err << "Invalid schema. 'precision' and 'scale' "
              << "must be set for DECIMAL type. Type="
              << apache::thrift::ThriftDebugString(schema.cols[i].type);
          return Status(err.str());
        }
        ss << "DECIMAL(" << schema.cols[i].type.precision << ","
            << schema.cols[i].type.scale << ")";
        break;
      case recordservice::TTypeId::TIMESTAMP_NANOS:
        ss << "TIMESTAMP";
        break;
      default: {
        // FIXME: Add RCFile. We need to look in the file metadata for the number of
        // columns.
        stringstream err;
        err << "Invalid schema. Type "
            << apache::thrift::ThriftDebugString(schema.cols[i].type)
            << " is unknown.";
        return Status(err.str());
      }
    }
  }
  ss << ")";
  *sql = ss.str();
  return Status::OK();
}


static void PopulateResultSchema(const TResultSetMetadata& metadata,
    recordservice::TSchema* schema) {
  schema->cols.resize(metadata.columns.size());
  for (int i = 0; i < metadata.columns.size(); ++i) {
    ColumnType type = ColumnType::FromThrift(metadata.columns[i].columnType);
    schema->cols[i].type = ToRecordServiceType(type);
    schema->cols[i].name = metadata.columns[i].columnName;
  }
  if (metadata.__isset.is_count_star && metadata.is_count_star) {
    schema->is_count_star = true;
  }
}

void ImpalaServer::GetProtocolVersion(recordservice::TProtocolVersion& return_val) {
  shared_ptr<SessionState> session;
  const TUniqueId& session_id = ThriftServer::GetThreadConnectionId();
  Status status = GetSessionState(session_id, &session);
  if (!status.ok()) {
    ThrowRecordServiceException(recordservice::TErrorCode::INTERNAL_ERROR,
        "Could not get session.", status.msg().GetFullMessageDetails());
  }
  if (session->session_type == TSessionType::RECORDSERVICE_PLANNER) {
    CheckConnectionLimit(RecordServiceMetrics::NUM_OPEN_PLANNER_SESSIONS->value(),
        recordservice_planner_server_->num_worker_threads(),
        RECORD_SERVICE_PLANNER_SERVER_NAME);
  } else if (session->session_type == TSessionType::RECORDSERVICE_WORKER) {
    CheckConnectionLimit(RecordServiceMetrics::NUM_OPEN_WORKER_SESSIONS->value(),
        recordservice_worker_server_->num_worker_threads(),
        RECORD_SERVICE_WORKER_SERVER_NAME);
  } else {
    stringstream ss;
    ss << "Unexpected session type: " << session->session_type;
    ThrowRecordServiceException(recordservice::TErrorCode::INTERNAL_ERROR, ss.str());
  }
  return_val = "1.0";
}

TExecRequest ImpalaServer::PlanRecordServiceRequest(
    const recordservice::TPlanRequestParams& req, scoped_ptr<re2::RE2>* path_filter,
    QueryStateRecord* record) {
  RecordServiceMetrics::NUM_PLAN_REQUESTS->Increment(1);
  record->num_rows_fetched = 0;
  record->process_nominator = 0;
  record->process_denominator = 0;
  record->default_db = "default";
  record->stmt_type = TStmtType::PLAN;

  if (IsOffline()) {
    ThrowRecordServiceException(recordservice::TErrorCode::SERVICE_BUSY,
        "This RecordServicePlanner is not ready to accept requests."
        " Retry your request later.");
  }

  TQueryCtx query_ctx;
  PrepareQueryContext(&query_ctx);
  query_ctx.__set_is_record_service_request(true);

  // Setting num_nodes = 1 means we generate a single node plan which has
  // a simpler structure. It also prevents Impala from analyzing multi-table
  // queries, i.e. joins.
  query_ctx.request.query_options.__set_num_nodes(1);

  // Disable codegen. Codegen works well for Impala because each fragment processes
  // multiple blocks, so the cost of codegen is amortized.
  // TODO: implement codegen caching.
  query_ctx.request.query_options.__set_disable_codegen(true);
  if (req.options.__isset.abort_on_corrupt_record) {
    query_ctx.request.query_options.__set_abort_on_error(
        req.options.abort_on_corrupt_record);
  }

  // Populate session information. This includes, among other things, the user
  // information.
  shared_ptr<SessionState> session;
  const TUniqueId& session_id = ThriftServer::GetThreadConnectionId();
  Status status = GetSessionState(session_id, &session);
  if (!status.ok()) {
    ThrowRecordServiceException(recordservice::TErrorCode::INTERNAL_ERROR,
        "Could not get session.", status.msg().GetFullMessageDetails());
  }
  DCHECK(session != NULL);
  // Set the user name. If it was set by a lower-level transport (i.e authenticated
  // user), use that. Otherwise, use the value in the request.
  const ThriftServer::Username& username =
      ThriftServer::GetThreadConnectionContext()->username;
  if (username.empty()) {
    session->connected_user = req.user;
  } else {
    session->connected_user = username;
  }

  unique_lock<mutex> tmp_tbl_lock;

  switch (req.request_type) {
    case recordservice::TRequestType::Sql:
      query_ctx.request.stmt = req.sql_stmt;
      break;
    case recordservice::TRequestType::Path: {

      // TODO: improve tmp table management or get impala to do it properly.
      unique_lock<mutex> l(tmp_tbl_lock_);
      tmp_tbl_lock.swap(l);

      string tmp_table;
      THdfsFileFormat::type format;
      Status status = CreateTmpTable(req, &tmp_table, path_filter, &format);
      if (!status.ok()) {
        ThrowRecordServiceException(recordservice::TErrorCode::INVALID_REQUEST,
            "Could not create temporary table.",
            status.msg().GetFullMessageDetails());
      }
      if (req.path.__isset.query) {
        if (format != THdfsFileFormat::TEXT) {
          ThrowRecordServiceException(recordservice::TErrorCode::INVALID_REQUEST,
              "Query request currently only supports TEXT files.");
        }
        string query = req.path.query;
        size_t p = req.path.query.find("__PATH__");
        if (p == string::npos) {
          ThrowRecordServiceException(recordservice::TErrorCode::INVALID_REQUEST,
              "Query request must contain __PATH__: " + query);
        }
        query.replace(p, 8, tmp_table);
        query_ctx.request.stmt = query;
      } else {
        query_ctx.request.stmt = "SELECT * FROM " + tmp_table;
      }
      break;
    }
    default:
      ThrowRecordServiceException(recordservice::TErrorCode::INVALID_REQUEST,
          "Unsupported request types. Supported request types are: SQL");
  }

  record->stmt = query_ctx.request.stmt;
  VLOG_REQUEST << "RecordService::PlanRequest: " << query_ctx.request.stmt;

  session->ToThrift(session_id, &query_ctx.session);
  record->effective_user = session->connected_user;

  // Plan the request.
  TExecRequest result;
  status = exec_env_->frontend()->GetRecordServiceExecRequest(query_ctx, &result);
  if (tmp_tbl_lock.owns_lock()) tmp_tbl_lock.unlock();
  if (!status.ok()) {
    ThrowRecordServiceException(recordservice::TErrorCode::INVALID_REQUEST,
        "Could not plan request.",
        status.msg().GetFullMessageDetails());
  }
  if (result.stmt_type != TStmtType::QUERY) {
    ThrowRecordServiceException(recordservice::TErrorCode::INVALID_REQUEST,
        "Cannot run non-SELECT statements");
  }

  return result;
}

// Returns the regex to match a file pattern. e.g. "*.java" -> "(.*)\.java"
// TODO: is there a library for doing this?
string FilePatternToRegex(const string& pattern) {
  string result;
  for (int i = 0; i < pattern.size(); ++i) {
    char c = pattern.at(i);
    if (c == '*') {
      result.append("(.*)");
    } else if (c == '.') {
      result.append("\\.");
    } else {
      result.append(1, c);
    }
  }
  return result;
}

Status ReadFileHeader(hdfsFS fs, const char* path,
    uint8_t header[HADOOP_FILE_HEADER_SIZE], int* bytes_read) {
  hdfsFile file = hdfsOpenFile(fs, path, O_RDONLY, HADOOP_FILE_HEADER_SIZE, 0, 0);
  if (file == NULL) return Status("Could not open file.");
  *bytes_read = hdfsRead(fs, file, header, HADOOP_FILE_HEADER_SIZE);
  hdfsCloseFile(fs, file);
  return Status::OK();
}

// TODO: move this logic to the planner? Not clear if that is easier.
Status DetermineFileFormat(hdfsFS fs, const string& path,
    const re2::RE2* path_filter, THdfsFileFormat::type* format, string* first_file) {
  // Default to text.
  *format = THdfsFileFormat::TEXT;
  int num_entries = 1;
  hdfsFileInfo* files = hdfsListDirectory(fs, path.c_str(), &num_entries);
  if (files == NULL) return Status("Could not list directory.");

  Status status;

  // Look for the first name that matches the path and filter. We'll look at
  // the file format of that file. This doesn't handle the case where a directory
  // is mixed format.
  // TODO: think about that.
  for (int i = 0; i < num_entries; ++i) {
    if (files[i].mKind != kObjectKindFile) continue;
    if (path_filter != NULL) {
      char* base_filename = strrchr(files[i].mName, '/');
      if (base_filename == NULL) {
        base_filename = files[i].mName;
      } else {
        base_filename += 1;
      }
      if (!re2::RE2::FullMatch(base_filename, *path_filter)) continue;
    }

    uint8_t header[HADOOP_FILE_HEADER_SIZE];
    int bytes_read;
    status = ReadFileHeader(fs, files[i].mName, header, &bytes_read);
    if (!status.ok()) break;

    if (bytes_read == HADOOP_FILE_HEADER_SIZE) {
      if (memcmp(header, AVRO_HEADER, HADOOP_FILE_HEADER_SIZE) == 0) {
        *format = THdfsFileFormat::AVRO;
      } else if (memcmp(header, PARQUET_HEADER, HADOOP_FILE_HEADER_SIZE) == 0) {
        *format = THdfsFileFormat::PARQUET;
      } else if (memcmp(header, SEQUENCE_HEADER, HADOOP_FILE_HEADER_SIZE) == 0) {
        *format = THdfsFileFormat::SEQUENCE_FILE;
      } else if (memcmp(header, RCFILE_HEADER, HADOOP_FILE_HEADER_SIZE) == 0) {
        *format = THdfsFileFormat::RC_FILE;
      }
    }
    *first_file = files[i].mName;
    break;
  }
  hdfsFreeFileInfo(files, num_entries);
  return status;
}

// Representation of a combined task.
// It contains the list of hosts they have in common (index into all_hosts)
// and the index of ranges that are part of this task (index into scan_ranges)
struct CombinedTask {
  vector<int> local_hosts;
  vector<int> ranges;

  CombinedTask() {}

  CombinedTask(const TScanRangeLocations& scan_range, int idx) {
    ranges.push_back(idx);
    for (int i = 0; i < scan_range.locations.size(); ++i) {
      local_hosts.push_back(scan_range.locations[i].host_idx);
    }
  }
};

// Combine scan_ranges. We want to balance locality and evenness of task sizes.
// max_tasks is the suggested maximum number of tasks to generate. The actual
// number can either be higher or lower. On return, this returns a list of
// combined tasks.
// This current algorithm only combines scan ranges that share two local hosts
// and leaves the remaining as uncombined.
// TODO: explore alternate ways to do this.
void CombineTasks(const vector<TScanRangeLocations>& scan_ranges,
    int max_tasks, vector<CombinedTask>* tasks) {
  VLOG_QUERY << "Combining " << scan_ranges.size()
             << " tasks. max_tasks=" << max_tasks;
  if (scan_ranges.size() <= max_tasks || max_tasks <= 0) {
    for (int i = 0; i < scan_ranges.size(); ++i) {
      tasks->push_back(CombinedTask(scan_ranges[i], i));
    }
    return;
  }

  // This is the maximum number of scan ranges we will put into one task. This
  // is never violated and we will never have combined tasks bigger than this.
  const int max_num_tasks = BitUtil::Ceil(scan_ranges.size(), max_tasks);

  // The maximum size (in bytes scanned) of a combined task. We will stop adding
  // to a combined task if:
  // 1. It reduces parallelism too much (greater than max_num_tasks)
  // 2. Or is too big (max_task_size).
  int64_t max_task_size = FLAGS_max_task_size;
  if (max_task_size < 0) max_task_size = std::numeric_limits<int64_t>::max();

  // Set to true if ranges[i] has been assigned to a task.
  vector<bool> assigned;
  assigned.resize(scan_ranges.size());

  // This is a mapping of <host1, host2> -> set of ranges that are local on those
  // hosts. This is the core data structure for the algorithm.
  unordered_map<pair<int, int>, unordered_set<int> > state;

  // First, loop through each range and add to state all the pairs of host it
  // is part of. For example, if the range is on hosts 4, 6, 7
  // this would add to state (4, 6), (4, 7), (6, 7).
  // The sorting guarantees that that in each pair, pair.first < pair.second.
  for (int i = 0; i < scan_ranges.size(); ++i) {
    vector<int> hosts;
    for (int j = 0; j < scan_ranges[i].locations.size(); ++j) {
      hosts.push_back(scan_ranges[i].locations[j].host_idx);
    }
    sort(hosts.begin(), hosts.end());

    for (int j = 0; j < hosts.size(); ++j) {
      for (int k = j + 1; k < hosts.size(); ++k) {
        pair<int, int> r(hosts[j], hosts[k]);
        state[r].insert(i);
      }
    }
  }

  // Step two: keeping looping through state until it is empty. Each entry in
  // state contains a potential combined task. The key for the entry is the
  // host pair and the value is the list of ranges that is located on both hosts.
  // The range might have already been assigned though.
  //
  // In each iteration we check:
  //  1. If the remaining number of ranges is greater than the task size (skipping
  //     assigned ranges), generate a combined task with a random subset (of task
  //     size) of the ranges. This marks of all those ranges assigned.
  //  2. If the number of remaining ranges in the entry is below task_size, remove
  //     this entry from state.
  // This guarantees that for each entry, we decrease the number of ranges in it,
  // until it is smaller than task size, in which case we remove it from state.
  // This time complexity of this should be O(num_ranges * num_replicas^2), that is
  // we revisit each range once for each pair of hosts it is on.
  //
  // We don't want to generate as many combined tasks from one entry to prevent
  // skew (it means the first entry we walk in state will get more tasks). By
  // iterating over the entries, we guarantee other ones get a chance to assign
  // the ranges.
  unordered_map<pair<int, int>, unordered_set<int> >::iterator it;
  vector<int> colocated_ranges;
  while (state.size() > 0) {
    for (it = state.begin(); it != state.end();) {
      // Store the current it and advance it. This is because we might remove the
      // current entry and need to advance it first.
      const pair<int, int>& host_indices = it->first;
      unordered_set<int>& ranges = it->second;
      unordered_map<pair<int, int>, unordered_set<int> >::iterator state_it = it++;

      // Collect the ranges that are still unassigned. This also removes from ranges
      // the ones that are assigned.
      colocated_ranges.clear();
      for (unordered_set<int>::iterator ranges_it = ranges.begin();
          ranges_it != ranges.end();) {
        int idx = *ranges_it;
        unordered_set<int>::iterator curr_it = ranges_it++;
        if (assigned[idx]) {
          // This is an optimization to GC the ranges on this host pair. If we
          // end up revisiting this entry a lot, this is helpful. It is not required
          // for correctness though (we will still remove the entry based on the
          // number of colocated_ranges left).
          ranges.erase(curr_it);
          continue;
        }
        colocated_ranges.push_back(idx);
      }

      int num_colocated_ranges = colocated_ranges.size();
      if (num_colocated_ranges > 1) {
        // The entry contains enough ranges to generate a combined task.
        random_shuffle(colocated_ranges.begin(), colocated_ranges.end());
        CombinedTask task;
        task.local_hosts.push_back(host_indices.first);
        task.local_hosts.push_back(host_indices.second);
        int tasks_to_combine = ::min(num_colocated_ranges, max_num_tasks);
        int64_t combined_task_size = 0;
        for (int i = 0; i < tasks_to_combine; ++i) {
          int range_idx = colocated_ranges[i];
          DCHECK(!assigned[range_idx]);
          DCHECK(scan_ranges[range_idx].scan_range.__isset.hdfs_file_split);
          int64_t range_length = scan_ranges[range_idx].scan_range.hdfs_file_split.length;
          if (i > 0 && combined_task_size + range_length > max_task_size) {
            // The combined task is too large in bytes, stop combining.
            // TODO: this is greedy, could replace with 0-1 knapsack.
            break;
          }
          assigned[range_idx] = true;
          task.ranges.push_back(range_idx);
          combined_task_size += range_length;
        }
        tasks->push_back(task);
        num_colocated_ranges -= task.ranges.size();
      }

      if (num_colocated_ranges <= 1) {
        state.erase(state_it);
        continue;
      }
    }
  }

  // For the remaining unassigned ranges, just generate single range
  // tasks.
  // TODO: not right. We can also combine these arbitrary.
  for (int i = 0; i < scan_ranges.size(); ++i) {
    if (assigned[i]) continue;
    tasks->push_back(CombinedTask(scan_ranges[i], i));
  }
}

//
// RecordServicePlanner
//
void ImpalaServer::PlanRequest(recordservice::TPlanRequestResult& return_val,
  const recordservice::TPlanRequestParams& req) {
  QueryStateRecord record;
  record.start_time = TimestampValue::LocalTime();
  try {
    scoped_ptr<re2::RE2> path_filter;
    TExecRequest result = PlanRecordServiceRequest(req, &path_filter, &record);

    {
      shared_lock<shared_mutex> l(recordservice_membership_lock_);
      return_val.hosts = known_recordservice_worker_addresses_;
    }

    // Fail the plan request if the worker membership is empty.
    // This is probably due to ZK membership not populated properly.
    if (return_val.hosts.size() == 0) {
      ThrowRecordServiceException(recordservice::TErrorCode::INVALID_REQUEST,
          "Worker membership is empty. Please ensure all RecordService Worker "
          "nodes are running.");
    }

    // Extract the types of the result.
    DCHECK(result.__isset.result_set_metadata);
    PopulateResultSchema(result.result_set_metadata, &return_val.schema);

    // Walk the plan to compute the tasks. We want to find the scan ranges and
    // convert them into tasks.
    // Impala, for these queries, will generate one fragment (with a single scan node)
    // and have all the scan ranges in that scan node. We want to generate one task
    // (with the fragment) for each scan range.
    DCHECK(result.__isset.query_exec_request);
    TQueryExecRequest& query_request = result.query_exec_request;
    DCHECK_EQ(query_request.per_node_scan_ranges.size(), 1);
    vector<TScanRangeLocations> scan_ranges;
    const int64_t scan_node_id = query_request.per_node_scan_ranges.begin()->first;
    scan_ranges.swap(query_request.per_node_scan_ranges.begin()->second);

    // FIXME: log audit events. Is there right? Should we log this on the worker?
    //if (IsAuditEventLoggingEnabled()) {
    //  LogAuditRecord(*(exec_state->get()), *(request));
    //}
    result.access_events.clear();

    return_val.request_id.hi = query_request.query_ctx.query_id.hi;
    return_val.request_id.lo = query_request.query_ctx.query_id.lo;
    query_request.__isset.record_service_task_id = true;

    // Send analysis warning as part of TPlanRequestResult.
    for (int i = 0; i < result.analysis_warnings.size(); ++i) {
      recordservice::TLogMessage msg;
      msg.message = result.analysis_warnings[i];
      return_val.warnings.push_back(msg);
    }
    result.analysis_warnings.clear();

    // Empty scan, just return. No tasks to generate.
    if (scan_ranges.empty()) return;

    // The TRecordServiceExecRequest will contain a bunch of TQueryRequest
    // objects.. each corresponding to a PlanFragment. This is then reconstituted
    // into a list of TExecRequests (again one for each PlanFragment). Each
    // TExecRequest is then serialized and set as the "task" field of the
    // TTask object.
    // To do this we:
    //  1. Copy the original request
    //  2. Modify it so it contains just enough information for the scan range it is for.
    //  3. Reserialize and compress it.
    int buffer_size = 100 * 1024;  // start out with 100KB
    ThriftSerializer serializer(true, buffer_size);

    TGetMasterKeyRequest key_request;
    TGetMasterKeyResponse key_response;
    if (!FLAGS_principal.empty()) {
      // Get the master key, and use it to encrypt the TExecRequest
      // Since we are requesting the latest master key here, use a negative number.
      key_request.seq = -1;
      Status status = exec_env_->frontend()->GetMasterKey(key_request, &key_response);
      if (!status.ok()) {
        ImpalaServer::ThrowRecordServiceException(
            recordservice::TErrorCode::INTERNAL_ERROR,
            "Error while getting the master key.",
            status.msg().GetFullMessageDetails());
      }
    }

    scoped_ptr<Codec> compressor;
    Codec::CreateCompressor(NULL, false, THdfsCompression::LZ4, &compressor);

    // Collect all references partitions and remove them from the 'result' request
    // object. Each task will only reference a single partition and we don't want
    // to send the rest.
    map<int64_t, THdfsPartition> all_partitions;
    DCHECK_EQ(query_request.desc_tbl.tableDescriptors.size(), 1);
    if (query_request.desc_tbl.tableDescriptors[0].__isset.hdfsTable) {
      all_partitions.swap(
          query_request.desc_tbl.tableDescriptors[0].hdfsTable.partitions);
    }

    // Do the same for hosts.
    vector<TNetworkAddress> all_hosts;
    all_hosts.swap(query_request.host_list);
    ResolveRecordServiceWorkerPorts(&all_hosts);

    // At this point we want to convert scan ranges to tasks. The default behavior
    // is to generate 1:1 mapping. However if the number of scan ranges is very large,
    // it can be beneficial to combine them. This reduces task launch overhead as
    // well as the amount of metadata being distributed across the cluster.
    // The downside of combining tasks is:
    //  1. Reduced parallelism
    //  2. More work to do when there are failures (tasks are bigger)
    //  3. Reduced locality. The chance of combining two tasks so that they share
    //     all the local replicas is very unlikely.
    // To mitigate 1 and 2, we only, by default, combine tasks when there are very
    // many tasks.
    // To mitigate 3, we combine tasks that share two hosts preferentially.
    // The optimal combination is likely NP complete so we will use a greedy solution
    // instead.

    // This vector contains a list of combined tasks and will be <= scan_ranges.size()
    vector<CombinedTask> combined_tasks;
    int max_tasks = req.__isset.max_tasks ? req.max_tasks : FLAGS_max_tasks;
    if (max_tasks < 0) max_tasks = FLAGS_max_tasks;
    if (max_tasks < 0) {
      // max tasks was not set by the plan request or the server default, compute it
      // from the membership.
      max_tasks = return_val.hosts.size() * CpuInfo::num_cores() * FLAGS_tasks_per_core;
      if (max_tasks < 0) max_tasks = 0;
    }
    CombineTasks(scan_ranges, max_tasks, &combined_tasks);

    for (int i = 0; i < combined_tasks.size(); ++i) {
      recordservice::TTask task;
      task.task_size = combined_tasks[i].ranges.size();
      // Only scans of a single range can be ordered
      task.results_ordered = (task.task_size == 1);

      // Generate the task id from the request id. Just increment the lo field. It
      // doesn't matter if this overflows. Return the task ID to the RecordService
      // client as well as setting it in the plan request.
      task.task_id.hi = return_val.request_id.hi;
      task.task_id.lo = return_val.request_id.lo + i + 1;
      query_request.record_service_task_id.hi = task.task_id.hi;
      query_request.record_service_task_id.lo = task.task_id.lo;

      // Populate the hosts. We maintain a mapping of global to local host indices. The
      // global index is the index into all hosts and the local is an index into
      // task.local_hosts which only contains the hosts for this task.
      map<int, int> global_to_local_hosts;
      query_request.host_list.clear();
      for (int j = 0; j < combined_tasks[i].local_hosts.size(); ++j) {
        int global_idx = combined_tasks[i].local_hosts[j];
        const TNetworkAddress& addr = all_hosts[global_idx];

        // This scan range is not from a DN (e.g. S3). There are no-local
        // ranges.
        if (!addr.__isset.hdfs_host_name) continue;

        recordservice::TNetworkAddress host;
        host.hostname = addr.hdfs_host_name;
        host.port = addr.port;

        if (host.port == -1) {
          // This indicates that we have DNs where there is no RecordService
          // worker running. Don't set those nodes as being local.
          VLOG(2) << "No RecordService worker running on host: " << host.hostname;
          continue;
        }

        if (global_to_local_hosts.find(global_idx) == global_to_local_hosts.end()) {
          query_request.host_list.push_back(addr);
          int idx = task.local_hosts.size();
          task.local_hosts.push_back(host);
          global_to_local_hosts[global_idx] = idx;
        }
      }

      // Clear the partitions list, we will just add the ones that are referenced.
      query_request.desc_tbl.tableDescriptors[0].hdfsTable.partitions.clear();
      query_request.per_node_scan_ranges.clear();
      for (int j = 0; j < combined_tasks[i].ranges.size(); ++j) {
        TScanRangeLocations& scan_range = scan_ranges[combined_tasks[i].ranges[j]];
        if (scan_range.scan_range.__isset.hdfs_file_split) {
          if (path_filter != NULL) {
            const string& base_filename = scan_range.scan_range.hdfs_file_split.file_name;
            if (!re2::RE2::FullMatch(base_filename, *path_filter.get())) {
              VLOG_FILE << "File '" << base_filename
                        << "' did not match pattern: '" << req.path.path << "'";
              continue;
            }
          }
        } else {
          // Only HDFS tasks are ordered since we single thread the scanner.
          task.results_ordered = false;
        }

        // Remap the tasks host index.
        vector<TScanRangeLocation> local_hosts;
        local_hosts.swap(scan_range.locations);
        scan_range.locations.clear();
        for (int k = 0; k < local_hosts.size(); ++k) {
          if (global_to_local_hosts.find(local_hosts[k].host_idx) ==
              global_to_local_hosts.end()) {
            // After combining, this replica is no longer considered local.
            continue;
          }
          local_hosts[k].host_idx = global_to_local_hosts[local_hosts[k].host_idx];
          scan_range.locations.push_back(local_hosts[k]);
        }

        if (scan_range.locations.size() == 0) {
          // In this case, this scan range does not live on any DN (e.g. S3). Fabricate
          // a host name so that the BE can execute it. Locality does not matter.
          // TODO: revisit how we deal with this. The FE normally does this for Impala
          // (but that might be a questionable choice as well).
          DCHECK_EQ(query_request.host_list.size(), 0);
          TNetworkAddress fabricated_host;
          fabricated_host.hostname = "remote*addr";
          fabricated_host.port = 0;
          query_request.host_list.push_back(fabricated_host);

          TScanRangeLocation fabricated_loation;
          fabricated_loation.host_idx = 0;
          scan_range.locations.push_back(fabricated_loation);
        }

        // Populate the partition descriptors for the referenced partitions.
        int64_t id = scan_range.scan_range.hdfs_file_split.partition_id;
        query_request.desc_tbl.tableDescriptors[0].hdfsTable.partitions[id] =
            all_partitions[id];

        // Add the range
        query_request.per_node_scan_ranges[scan_node_id].push_back(scan_range);
      }
      if (query_request.per_node_scan_ranges[scan_node_id].empty()) {
        continue;
      }

      // Now construct an encrypted TRecordServiceTask
      TRecordServiceTask rs_task;
      string serialized_request;
      serializer.Serialize<TExecRequest>(&result, &serialized_request);
      compressor->Compress(serialized_request, true, &rs_task.request);
      if (!FLAGS_principal.empty()) {
        rs_task.seq = key_response.seq;
        rs_task.hmac = ComputeHMAC(key_response.key, rs_task.request);
        rs_task.__isset.seq = true;
        rs_task.__isset.hmac = true;
      }
      serializer.Serialize<TRecordServiceTask>(&rs_task, &task.task);
      return_val.tasks.push_back(task);
    }
  } catch (const recordservice::TRecordServiceException& e) {
    RecordServiceMetrics::NUM_FAILED_PLAN_REQUESTS->Increment(1);
    record.end_time = TimestampValue::LocalTime();
    record.query_state = beeswax::QueryState::EXCEPTION;
    ArchiveRecord(record);
    throw e;
  }
  record.end_time = TimestampValue::LocalTime();
  record.query_state = beeswax::QueryState::FINISHED;
  ArchiveRecord(record);
}

void ImpalaServer::GetSchema(recordservice::TGetSchemaResult& return_val,
      const recordservice::TPlanRequestParams& req) {
  RecordServiceMetrics::NUM_GET_SCHEMA_REQUESTS->Increment(1);
  try {
    // TODO: fix this to not do the whole planning.
    scoped_ptr<re2::RE2> dummy;
    QueryStateRecord dummy_record;
    TExecRequest result = PlanRecordServiceRequest(req, &dummy, &dummy_record);
    DCHECK(result.__isset.result_set_metadata);
    PopulateResultSchema(result.result_set_metadata, &return_val.schema);
  } catch (const recordservice::TRecordServiceException& e) {
    RecordServiceMetrics::NUM_FAILED_GET_SCHEMA_REQUESTS->Increment(1);
    throw e;
  }
}

//
// RecordServiceWorker
//
void ImpalaServer::ExecTask(recordservice::TExecTaskResult& return_val,
    const recordservice::TExecTaskParams& req) {
  RecordServiceMetrics::NUM_TASK_REQUESTS->Increment(1);
  try {
    if (IsOffline()) {
      ThrowRecordServiceException(recordservice::TErrorCode::SERVICE_BUSY,
          "This RecordServicePlanner is not ready to accept requests."
          " Retry your request later.");
    }

    ScopedSessionState session_handle(this);
    GetRecordServiceSession(&session_handle);

    shared_ptr<RecordServiceTaskState> task_state(new RecordServiceTaskState());

    scoped_ptr<Codec> decompressor;
    Codec::CreateDecompressor(NULL, false, THdfsCompression::LZ4, &decompressor);

    TRecordServiceTask rs_task;
    uint32_t size = req.task.size();
    Status status = DeserializeThriftMsg(
        reinterpret_cast<const uint8_t*>(req.task.data()), &size, true, &rs_task);
    if (!status.ok()) {
      ThrowRecordServiceException(recordservice::TErrorCode::INVALID_TASK,
          "Task is corrupt.",
          status.msg().GetFullMessageDetails());
    }

    // Verify the task is not modified
    if (!FLAGS_principal.empty()) {
      TGetMasterKeyRequest key_request;
      TGetMasterKeyResponse key_response;
      if (!rs_task.__isset.seq || !rs_task.__isset.hmac) {
        ThrowRecordServiceException(recordservice::TErrorCode::AUTHENTICATION_ERROR,
            "Kerberos is enabled but task is not encrypted.");
      }
      key_request.seq = rs_task.seq;
      status = exec_env_->frontend()->GetMasterKey(key_request, &key_response);
      if (!status.ok()) {
        ImpalaServer::ThrowRecordServiceException(
            recordservice::TErrorCode::INTERNAL_ERROR,
            "Error while getting the master key.",
            status.msg().GetFullMessageDetails());
      }

      DCHECK_EQ(key_response.seq, rs_task.seq);

      string hmac = ComputeHMAC(key_response.key, rs_task.request);
      if (strcmp(hmac.c_str(), rs_task.hmac.c_str()) != 0) {
        ThrowRecordServiceException(recordservice::TErrorCode::INVALID_TASK,
            "Task failed authentication.",
            "Task's HMAC doesn't match the one used to encrypt it.");
      }
      VLOG_REQUEST << "Successfully verified the task's HMAC";
    } else {
      // non-secure cluster - make sure hmac/seq is not set
      if (rs_task.__isset.seq || rs_task.__isset.hmac) {
        ThrowRecordServiceException(recordservice::TErrorCode::AUTHENTICATION_ERROR,
            "Kerberos is not enabled but task is encrypted.");
      }
    }

    string decompressed_exec_req;
    status = decompressor->Decompress(rs_task.request, true, &decompressed_exec_req);
    if (!status.ok()) {
      ThrowRecordServiceException(recordservice::TErrorCode::INVALID_TASK,
          "Task is corrupt.",
          status.msg().GetFullMessageDetails());
    }

    TExecRequest exec_req;
    size = decompressed_exec_req.size();
    status = DeserializeThriftMsg(
        reinterpret_cast<const uint8_t*>(decompressed_exec_req.data()),
        &size, true, &exec_req);
    if (!status.ok()) {
      ThrowRecordServiceException(recordservice::TErrorCode::INVALID_TASK,
          "Task is corrupt.",
          status.msg().GetFullMessageDetails());
    }

    TQueryExecRequest& query_request = exec_req.query_exec_request;

    VLOG_REQUEST << "RecordService::ExecRequest: "
                 << query_request.query_ctx.request.stmt;
    VLOG_QUERY << "RecordService::ExecRequest: query plan " << query_request.query_plan;

    // Verify the task as something we can run. We want to verify to support upgrade
    // scenarios more gracefully.
    if (query_request.fragments.size() != 1) {
      ThrowRecordServiceException(recordservice::TErrorCode::INVALID_TASK,
          "Only single fragment tasks are supported.");
    }
    TPlan& plan = query_request.fragments[0].plan;
    bool valid_task = true;
    if (plan.nodes.size() == 1) {
      // FIXME: support hbase too.
      if (plan.nodes[0].node_type != TPlanNodeType::HDFS_SCAN_NODE) valid_task = false;
    } else if (plan.nodes.size() == 2) {
      // Allow aggregation for count(*)
      if (plan.nodes[0].node_type != TPlanNodeType::AGGREGATION_NODE) valid_task = false;
    } else {
      valid_task = false;
    }
    if (!valid_task) {
      ThrowRecordServiceException(recordservice::TErrorCode::INVALID_TASK,
          "Only HDFS scan requests and count(*) are supported.");
    }

    // Set the options for this task by modifying the plan and setting query options.
    TPlanNode& plan_node = plan.nodes[0];
    task_state->fetch_size = DEFAULT_FETCH_SIZE;
    if (req.__isset.limit) plan_node.limit = req.limit;
    if (req.__isset.fetch_size) task_state->fetch_size = req.fetch_size;
    query_request.query_ctx.request.query_options.__set_batch_size(
        task_state->fetch_size);
    if (req.__isset.mem_limit) {
      // FIXME: this needs much more testing.
      query_request.query_ctx.request.query_options.__set_mem_limit(req.mem_limit);
    }
    if (req.__isset.offset && req.offset != 0) task_state->offset = req.offset;
    if (req.__isset.logging_level) {
      // Remap client logging levels to server logging levels.
      int level = 0;
      switch (req.logging_level) {
        case recordservice::TLoggingLevel::OFF:
          level = 0;
          break;

        case recordservice::TLoggingLevel::ALL:
        case recordservice::TLoggingLevel::TRACE:
          level = QUERY_VLOG_ROW_LEVEL;
          break;

        case recordservice::TLoggingLevel::FATAL:
        case recordservice::TLoggingLevel::ERROR:
          // Always on, no need to do anything.
          break;

        case recordservice::TLoggingLevel::WARN:
          level = QUERY_VLOG_WARNING_LEVEL;
          break;

        case recordservice::TLoggingLevel::INFO:
          level = QUERY_VLOG_FRAGMENT_LEVEL;
          break;

        case recordservice::TLoggingLevel::DEBUG:
          level = QUERY_VLOG_BATCH_LEVEL;
          break;
      }
      query_request.query_ctx.request.query_options.__set_logging_level(level);
    }

    shared_ptr<QueryExecState> exec_state;
    status = ExecuteRecordServiceRequest(&query_request.query_ctx,
        &exec_req, session_handle.get(), &exec_state);
    if (!status.ok()) {
      ThrowRecordServiceException(recordservice::TErrorCode::INVALID_TASK,
          "Could not execute task.",
          status.msg().GetFullMessageDetails());
    }
    PopulateResultSchema(*exec_state->result_metadata(), &return_val.schema);
    exec_state->SetRecordServiceTaskState(task_state);

    // Optimization if the result exprs are all just "simple" slot refs. This means
    // that they contain a single non-nullable tuple row. This is the common case and
    // we can simplify the row serialization logic.
    // TODO: this should be replaced by codegen to handle all the cases.
    bool all_slot_refs = true;
    const vector<ExprContext*>& output_exprs = exec_state->output_exprs();
    for (int i = 0; i < output_exprs.size(); ++i) {
      if (output_exprs[i]->root()->is_slotref()) {
        SlotRef* slot_ref = reinterpret_cast<SlotRef*>(output_exprs[i]->root());
        if (!slot_ref->tuple_is_nullable() && slot_ref->tuple_idx() == 0) continue;
      }
      all_slot_refs = false;
      break;
    }

    task_state->format = recordservice::TRecordFormat::Columnar;
    if (req.__isset.record_format) task_state->format = req.record_format;
    switch (task_state->format) {
      case recordservice::TRecordFormat::Columnar:
        task_state->results.reset(new RecordServiceParquetResultSet(
            task_state.get(), all_slot_refs, output_exprs));
        break;
      default:
        ThrowRecordServiceException(recordservice::TErrorCode::INVALID_REQUEST,
            "Service does not support this record format.");
    }
    task_state->results->Init(*exec_state->result_metadata(), task_state->fetch_size);

    exec_state->UpdateQueryState(QueryState::RUNNING);
    exec_state->WaitAsync();
    status = SetQueryInflight(session_handle.get(), exec_state);
    if (!status.ok()) {
      UnregisterQuery(exec_state->query_id(), false, &status);
    }
    return_val.handle.hi = exec_state->query_id().hi;
    return_val.handle.lo = exec_state->query_id().lo;
  } catch (const recordservice::TRecordServiceException& e) {
    RecordServiceMetrics::NUM_FAILED_TASK_REQUESTS->Increment(1);
    throw e;
  }
}

// Computes num/denom
static double ComputeProgress(RuntimeProfile::Counter* num,
    RuntimeProfile::Counter* denom) {
  if (num == NULL || denom == NULL || denom->value() == 0) return 0;
  double result = (double)num->value() / (double)denom->value();
  if (result > 1) result = 1;
  return result;
}

void ImpalaServer::Fetch(recordservice::TFetchResult& return_val,
    const recordservice::TFetchParams& req) {
  RecordServiceMetrics::NUM_FETCH_REQUESTS->Increment(1);
  try {
    TUniqueId query_id;
    query_id.hi = req.handle.hi;
    query_id.lo = req.handle.lo;

    shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
    if (exec_state.get() == NULL) {
      ThrowRecordServiceException(recordservice::TErrorCode::INVALID_HANDLE,
          "Invalid handle");
    }
    QUERY_VLOG_BATCH(exec_state->logger()) << "Fetch()";

    RecordServiceTaskState* task_state = exec_state->record_service_task_state();
    exec_state->BlockOnWait();

    task_state->results->SetReturnBuffer(&return_val);

    lock_guard<mutex> frl(*exec_state->fetch_rows_lock());
    lock_guard<mutex> l(*exec_state->lock());

    Status status = exec_state->FetchRows(
        task_state->fetch_size, task_state->results.get());
    if (!status.ok()) ThrowFetchException(status);

    if (!task_state->counters_initialized) {
      // First time the client called fetch. Extract the counters.
      RuntimeProfile* server_profile = exec_state->server_profile();

      task_state->serialize_timer = server_profile->GetCounter("RowMaterializationTimer");
      task_state->client_timer = server_profile->GetCounter("ClientFetchWaitTimer");

      RuntimeProfile* profile =
          GetHdfsScanNodeProfile(exec_state->coord()->query_profile());
      DCHECK_NOTNULL(profile);
      task_state->bytes_assigned_counter = profile->GetCounter("BytesAssigned");
      task_state->bytes_read_counter = profile->GetCounter("BytesRead");
      task_state->bytes_read_local_counter = profile->GetCounter("BytesReadLocal");
      task_state->rows_read_counter = profile->GetCounter("RowsRead");
      task_state->rows_returned_counter = profile->GetCounter("RowsReturned");
      task_state->decompression_timer = profile->GetCounter("DecompressionTime");
      task_state->hdfs_throughput_counter =
          profile->GetCounter("PerReadThreadRawHdfsThroughput");
      task_state->counters_initialized = true;
    }

    return_val.done = exec_state->eos();
    return_val.task_progress = ComputeProgress(
        task_state->bytes_read_counter, task_state->bytes_assigned_counter);
    return_val.record_format = task_state->format;

    task_state->results->FinalizeResult();
    RecordServiceMetrics::NUM_ROWS_FETCHED->Increment(return_val.num_records);
    QUERY_VLOG_BATCH(exec_state->logger())
        << "Fetched " << return_val.num_records << " records. Eos=" << return_val.done;
  } catch (const recordservice::TRecordServiceException& e) {
    RecordServiceMetrics::NUM_FAILED_FETCH_REQUESTS->Increment(1);
    throw e;
  }
}

void ImpalaServer::CloseTask(const recordservice::TUniqueId& req) {
  TUniqueId query_id;
  query_id.hi = req.hi;
  query_id.lo = req.lo;

  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
  if (exec_state.get() == NULL) return;

  RecordServiceMetrics::NUM_CLOSED_TASKS->Increment(1);
  Status status = CancelInternal(query_id, true);
  if (!status.ok()) return;
  UnregisterQuery(query_id, true);
}

// Macros to convert from runtime profile counters to metrics object.
// Also does unit conversion (i.e. STAT_MS converts to millis).
#define SET_STAT_MS_FROM_COUNTER(counter, stat_name)\
  if (counter != NULL) return_val.stats.__set_##stat_name(counter->value() / 1000000)

#define SET_STAT_FROM_COUNTER(counter, stat_name)\
  if (counter != NULL) return_val.stats.__set_##stat_name(counter->value())

// TODO: send back warnings from the runtime state. Impala doesn't generate them
// in the most useful way right now. Fix that.
void ImpalaServer::GetTaskStatus(recordservice::TTaskStatus& return_val,
      const recordservice::TUniqueId& req) {
  RecordServiceMetrics::NUM_GET_TASK_STATUS_REQUESTS->Increment(1);
  try {
    TUniqueId query_id;
    query_id.hi = req.hi;
    query_id.lo = req.lo;

    // TODO: should this grab the lock in GetQueryExecState()?
    shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id, false);
    if (exec_state.get() == NULL) {
      ThrowRecordServiceException(recordservice::TErrorCode::INVALID_HANDLE,
          "Invalid handle");
    }

    lock_guard<mutex> l(*exec_state->lock());

    RecordServiceTaskState* task_state = exec_state->record_service_task_state();
    if (!task_state->counters_initialized) {
      // Task hasn't started enough to have counters.
      return;
    }

    // Populate the results from the counters.
    return_val.stats.__set_task_progress(ComputeProgress(
        task_state->bytes_read_counter, task_state->bytes_assigned_counter));
    SET_STAT_MS_FROM_COUNTER(task_state->serialize_timer, serialize_time_ms);
    SET_STAT_MS_FROM_COUNTER(task_state->client_timer, client_time_ms);
    SET_STAT_FROM_COUNTER(task_state->bytes_read_counter, bytes_read);
    SET_STAT_FROM_COUNTER(task_state->bytes_read_local_counter, bytes_read_local);
    SET_STAT_FROM_COUNTER(task_state->rows_read_counter, num_records_read);
    SET_STAT_FROM_COUNTER(task_state->rows_returned_counter, num_records_returned);
    SET_STAT_MS_FROM_COUNTER(task_state->decompression_timer, decompress_time_ms);
    SET_STAT_FROM_COUNTER(task_state->hdfs_throughput_counter, hdfs_throughput);
  } catch (const recordservice::TRecordServiceException& e) {
    RecordServiceMetrics::NUM_FAILED_GET_TASK_STATUS_REQUESTS->Increment(1);
    throw e;
  }
}

Status ImpalaServer::CreateTmpTable(const recordservice::TPlanRequestParams& req,
    string* table_name, scoped_ptr<re2::RE2>* path_filter,
    THdfsFileFormat::type* format) {
  hdfsFS fs;
  Status status = HdfsFsCache::instance()->GetDefaultConnection(&fs);
  DCHECK_EQ(req.request_type, recordservice::TRequestType::Path);
  const recordservice::TPathRequest& request = req.path;
  if (!status.ok()) {
    // TODO: more error detail
    ThrowRecordServiceException(recordservice::TErrorCode::INTERNAL_ERROR,
        "Could not connect to HDFS");
  }

  bool is_directory = false;
  string path = request.path;
  string suffix;

  // First see if the path is a directory. This means the path does not need a
  // trailing '/' which is convenient.
  IsDirectory(fs, path.c_str(), &is_directory); // Drop status.
  if (!is_directory) {
    // TODO: this should do better globbing e.g. /path/*/a/b/*/. Impala has poor support
    // of this and requires more work.
    size_t last_slash = path.find_last_of('/');
    if (last_slash != string::npos) {
      suffix = path.substr(last_slash + 1);
      path = path.substr(0, last_slash);
    }

    status = IsDirectory(fs, path.c_str(), &is_directory);
    if (!status.ok()) {
      if (path.find('*') != string::npos) {
        // Path contains a * which we should (HDFS can) expand. e.g. /path/a.*/*
        ThrowRecordServiceException(recordservice::TErrorCode::INVALID_REQUEST,
            "Globbing is not yet supported: " + path);
      } else {
        ThrowRecordServiceException(recordservice::TErrorCode::INVALID_REQUEST,
            "No such file or directory: " + path);
      }
    }
  }

  if (!is_directory) {
    stringstream ss;
    ss << "Path must be a directory: " + path;
    // TODO: Impala should support LOCATIONs that are not directories.
    // Move the suffix and file filtering logic there.
    ThrowRecordServiceException(
        recordservice::TErrorCode::INVALID_REQUEST, ss.str());
  }

  // Check if the user has privilege to the path
  const ThriftServer::Username& username =
      ThriftServer::GetThreadConnectionContext()->username;
  TAuthorizePathRequest auth_path_request;
  TAuthorizePathResponse auth_path_response;
  if (username.empty()) {
    auth_path_request.username = req.user;
  } else {
    auth_path_request.username = username;
  }
  auth_path_request.path = path;
  RETURN_IF_ERROR(
      exec_env_->frontend()->AuthorizePath(auth_path_request, &auth_path_response));
  if (!auth_path_response.success) {
    stringstream ss;
    ss << "User " << auth_path_request.username << " does not have full access "
       << "to the path " << auth_path_request.path;
    ThrowRecordServiceException(
        recordservice::TErrorCode::AUTHENTICATION_ERROR, ss.str());
  }

  if (!suffix.empty()) path_filter->reset(new re2::RE2(FilePatternToRegex(suffix)));

  string first_file;
  RETURN_IF_ERROR(
      DetermineFileFormat(fs, path, path_filter->get(), format, &first_file));

  *table_name = string(TEMP_DB) + "." + string(TEMP_TBL);
  string create_tbl_stmt("CREATE EXTERNAL TABLE " + *table_name);

  // Append the schema to the create statement.
  if (request.__isset.schema) {
    // Schema is set from the client, use that.
    string sql_schema;
    RETURN_IF_ERROR(SchemaToSqlString(request.schema, &sql_schema));
    create_tbl_stmt += sql_schema + string(" ");
  } else {
    switch (*format) {
      case THdfsFileFormat::TEXT:
      case THdfsFileFormat::SEQUENCE_FILE:
        // For text and sequence file, we can't get the schema, just make it a string.
        create_tbl_stmt += string("(record STRING) ");
        break;
      case THdfsFileFormat::PARQUET:
        create_tbl_stmt += string(" LIKE PARQUET '") + first_file + string("' ");
        break;
      case THdfsFileFormat::AVRO:
        create_tbl_stmt += string(" LIKE AVRO '") + first_file + string("' ");
        break;
      default: {
        // FIXME: Add RCFile. We need to look in the file metadata for the number of
        // columns.
        stringstream ss;
        ss << "Cannot infer schema for file format '" << *format;
        return Status(ss.str());
      }
    }
  }

  // Append the file format.
  switch (*format) {
    case THdfsFileFormat::TEXT:
      if (request.__isset.field_delimiter) {
        create_tbl_stmt += string("ROW FORMAT DELIMITED FIELDS TERMINATED BY '")
          + (char)request.field_delimiter + string("' ");
      }
      create_tbl_stmt += string("STORED AS TEXTFILE");
      break;
    case THdfsFileFormat::SEQUENCE_FILE:
      if (request.__isset.field_delimiter) {
        create_tbl_stmt += string("ROW FORMAT DELIMITED FIELDS TERMINATED BY '")
          + (char)request.field_delimiter + string("' ");
      }
      create_tbl_stmt += string("STORED AS SEQUENCEFILE");
      break;
    case THdfsFileFormat::PARQUET:
      create_tbl_stmt += string("STORED AS PARQUET");
      break;
    case THdfsFileFormat::AVRO:
      create_tbl_stmt += string("STORED AS AVRO");
      break;
    case THdfsFileFormat::RC_FILE:
      create_tbl_stmt += string("STORED AS RCFILE");
      break;
    default: {
      stringstream ss;
      ss << "File format '" << *format << "'is not supported with path requests.";
      return Status(ss.str());
    }
  }
  create_tbl_stmt += " LOCATION \"" + path + "\"";

  // For now, we'll just always use one temp table.
  string commands[] = {
    "DROP TABLE IF EXISTS " + *table_name,
    "CREATE DATABASE IF NOT EXISTS " + string(TEMP_DB),
    create_tbl_stmt
  };

  ScopedSessionState session_handle(this);
  GetRecordServiceSession(&session_handle);
  shared_ptr<SessionState> session_state = session_handle.get();

  // We need to enforce restrictions on accesses to the temp table
  // but cannot give users access to it because the temp table is shared,
  // and one user may not be allowed to access the content of the table
  // created by another user.
  // Here we require that the user running the current process has full access to
  // the temp db and table, and all operations on the them should be done by
  // this user.
  // FIXME: this is a hack! think a better way to solve this issue.
  session_state->connected_user = getenv("USER");

  int num_commands = sizeof(commands) / sizeof(commands[0]);
  for (int i = 0; i < num_commands; ++i) {
    TQueryCtx query_ctx;
    query_ctx.request.stmt = commands[i];
    query_ctx.session.connected_user = session_state->connected_user;

    shared_ptr<QueryExecState> exec_state;
    RETURN_IF_ERROR(Execute(&query_ctx, session_state, &exec_state));
    exec_state->UpdateQueryState(QueryState::RUNNING);

    Status status = SetQueryInflight(session_state, exec_state);
    if (!status.ok()) {
      UnregisterQuery(exec_state->query_id(), false, &status);
      return status;
    }

    // block until results are ready
    exec_state->Wait();
    status = exec_state->query_status();
    if (!status.ok()) {
      UnregisterQuery(exec_state->query_id(), false, &status);
      return status;
    }
    exec_state->UpdateQueryState(QueryState::FINISHED);
    UnregisterQuery(exec_state->query_id(), true);
  }

  return Status::OK();
}

void ImpalaServer::GetMetric(recordservice::TMetricResponse& return_val,
    const string& key) {
  MetricGroup* metrics = exec_env_->metrics()->GetChildGroup("record-service");
  Metric* metric = metrics->FindMetricForTesting<Metric>(key);
  if (metric == NULL) return;
  return_val.__set_metric_string(metric->ToHumanReadable());
}

void ImpalaServer::GetDelegationToken(recordservice::TDelegationToken& token,
      const string& user, const string& renewer) {
  VLOG_REQUEST << "GetDelegationToken() user=" << user << " renewer=" << renewer;
  const ThriftServer::ConnectionContext* ctx = ThriftServer::GetThreadConnectionContext();
  if (ctx->mechanism_name != AuthManager::KERBEROS_MECHANISM) {
    stringstream ss;
    ss << "GetDelegationToken() can only be called with a Kerberos connection. ";
    if (ctx->mechanism_name.empty()) {
      ss << "Current connection is unsecure.";
    } else {
      ss << "Current connection mechanism is " << ctx->mechanism_name;
    }
    ImpalaServer::ThrowRecordServiceException(
        recordservice::TErrorCode::AUTHENTICATION_ERROR, ss.str());
  }
  DCHECK(!ctx->username.empty());

  TGetDelegationTokenRequest params;
  params.owner = ctx->username;
  params.user = user;
  params.renewer = renewer;

  TGetDelegationTokenResponse response;
  Status status = exec_env_->frontend()->GetDelegationToken(params, &response);
  if (!status.ok()) {
    ImpalaServer::ThrowRecordServiceException(
        recordservice::TErrorCode::AUTHENTICATION_ERROR,
        "Could not get delegation token.",
        status.GetDetail());
  }
  token.identifier = response.identifier;
  token.password = response.password;
  token.token = response.token;
}

void ImpalaServer::CancelDelegationToken(const recordservice::TDelegationToken& token) {
  VLOG_REQUEST << "CancelDelegationToken()";

  const ThriftServer::ConnectionContext* ctx = ThriftServer::GetThreadConnectionContext();
  if (ctx->mechanism_name.empty()) {
    ImpalaServer::ThrowRecordServiceException(
        recordservice::TErrorCode::AUTHENTICATION_ERROR,
        "CancelDelegationToken() can only be called from a secure connection.");
  }
  DCHECK(!ctx->username.empty());

  TCancelDelegationTokenRequest params;
  params.user = ctx->username;
  params.token = token.token;
  Status status = exec_env_->frontend()->CancelDelegationToken(params);
  if (!status.ok()) {
    ImpalaServer::ThrowRecordServiceException(
        recordservice::TErrorCode::AUTHENTICATION_ERROR,
        "Could not cancel delegation token.",
        status.GetDetail());
  }
}

void ImpalaServer::RenewDelegationToken(const recordservice::TDelegationToken& token) {
  VLOG_REQUEST << "RenewDelegationToken()";
  const ThriftServer::ConnectionContext* ctx = ThriftServer::GetThreadConnectionContext();
  if (ctx->mechanism_name != AuthManager::KERBEROS_MECHANISM) {
    stringstream ss;
    ss << "RenewDelegationToken() can only be called with a Kerberos connection. ";
    if (ctx->mechanism_name.empty()) {
      ss << "Current connection is unsecure.";
    } else {
      ss << "Current connection mechanism is " << ctx->mechanism_name;
    }
    ImpalaServer::ThrowRecordServiceException(
        recordservice::TErrorCode::AUTHENTICATION_ERROR, ss.str());
  }
  DCHECK(!ctx->username.empty());

  TRenewDelegationTokenRequest params;
  params.user = ctx->username;
  params.token = token.token;
  Status status = exec_env_->frontend()->RenewDelegationToken(params);
  if (!status.ok()) {
    ImpalaServer::ThrowRecordServiceException(
        recordservice::TErrorCode::AUTHENTICATION_ERROR,
        "Could not renew delegation token.",
        status.GetDetail());
  }
}

string ImpalaServer::ComputeHMAC(const string& key, const string& data) {
  unsigned char result[160];
  unsigned int result_len;
  HMAC(EVP_sha1(), reinterpret_cast<const unsigned char*>(key.c_str()), key.length(),
      reinterpret_cast<const unsigned char*>(data.c_str()), data.length(),
      reinterpret_cast<unsigned char*>(&result), &result_len);
  return string(reinterpret_cast<const char*>(result), result_len);
}

RuntimeProfile* ImpalaServer::GetHdfsScanNodeProfile(RuntimeProfile* coord_profile) {
  vector<RuntimeProfile*> children;
  coord_profile->GetAllChildren(&children);
  for (int i = 0; i < children.size(); ++i) {
    if (children[i]->name() == "HDFS_SCAN_NODE (id=0)") return children[i];
  }
  return NULL;
}

}
