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

#include "exec/hdfs-hive-serde-scanner.h"

#include <jni.h>
#include <string>

#include "exec/data-source-row-converter.h"
#include "exec/delimited-text-parser.h"
#include "exec/delimited-text-parser.inline.h"
#include "exec/external-data-source-executor.h"
#include "exec/hdfs-scan-node.h"
#include "rpc/jni-thrift-util.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "util/jni-util.h"

#include "gen-cpp/ExternalDataSource_types.h"

#include "common/names.h"

using namespace impala;

const char* HdfsHiveSerdeScanner::EXECUTOR_CLASS =
    "com/cloudera/impala/hive/serde/HiveSerDeExecutor";
const char* HdfsHiveSerdeScanner::EXECUTOR_CTOR_SIG = "([B)V";
const char* HdfsHiveSerdeScanner::EXECUTOR_DESERIALIZE_SIG = "([B)[B";
const char* HdfsHiveSerdeScanner::EXECUTOR_DESERIALIZE_NAME = "deserialize";

Status HdfsHiveSerdeScanner::IssueInitialRanges(
    HdfsScanNode* scan_node, const vector<HdfsFileDesc*>& files) {
  return HdfsTextScanner::IssueInitialRangesInternal(
      THdfsFileFormat::HIVE_SERDE, scan_node, files);
}

Status HdfsHiveSerdeScanner::InitNewRange() {
  HdfsPartitionDescriptor* hdfs_partition = context_->partition_descriptor();
  // We're passing it by pointer, and the value for variable will be
  // hold on stack (and perhaps overwritten) if it's non-static.
  static bool is_materialized_col = true;

  delimited_text_parser_.reset(new DelimitedTextParser(
      1, 0, &is_materialized_col, hdfs_partition->line_delim()));

  // Initialize the HiveSerDeExecutor
  TSerDeInit init_params;
  init_params.serde_class_name = hdfs_partition->serde_class_name();
  init_params.serde_properties = hdfs_partition->serde_properties();
  for (int i = scan_node_->num_partition_keys();
       i < scan_node_->hdfs_table()->num_cols(); ++i) {
    init_params.is_materialized.push_back(scan_node_->is_materialized_col()[i]);
  }

  JNIEnv* env = getJNIEnv();
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(env));

  jbyteArray init_params_bytes;
  RETURN_IF_ERROR(SerializeThriftMsg(env, &init_params, &init_params_bytes));

  // Create the java executor object with the serde class name
  // and properties associated with this HDFS partition
  executor_ = env->NewObject(executor_class_, executor_ctor_id_, init_params_bytes);
  RETURN_ERROR_IF_EXC(env);
  executor_ = env->NewGlobalRef(executor_);
  RETURN_ERROR_IF_EXC(env);

  RETURN_IF_ERROR(HdfsTextScanner::ResetScanner());

  return Status::OK();
}

HdfsHiveSerdeScanner::HdfsHiveSerdeScanner(HdfsScanNode* scan_node, RuntimeState* state)
  : HdfsTextScanner(scan_node, state),
    executor_(NULL), executor_class_(NULL), executor_ctor_id_(NULL),
    executor_deser_id_(NULL) {
}

HdfsHiveSerdeScanner::~HdfsHiveSerdeScanner() {
}

Status HdfsHiveSerdeScanner::Prepare(ScannerContext* context) {
  // Unlike a few other places, we don't call the same function from
  // HdfsScanner here, since field_locations_ is initialized with a different size.
  RETURN_IF_ERROR(HdfsScanner::Prepare(context));

  parse_delimiter_timer_ = ADD_CHILD_TIMER(scan_node_->runtime_profile(),
      "DelimiterParseTime", ScanNode::SCANNER_THREAD_TOTAL_WALLCLOCK_TIME);

  field_locations_.resize(state_->batch_size());
  row_end_locations_.resize(state_->batch_size());
  row_converter_.reset(new DataSourceRowConverter(
      scan_node_->tuple_desc(), template_tuple_, scan_node_->materialized_slots()));

  JNIEnv* env = getJNIEnv();
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(env));

  // Find out constructor and deserialize method id of the executor class.
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, EXECUTOR_CLASS, &executor_class_));
  executor_ctor_id_ = env->GetMethodID(executor_class_, "<init>", EXECUTOR_CTOR_SIG);
  RETURN_ERROR_IF_EXC(env);
  executor_deser_id_ = env->GetMethodID(
      executor_class_, EXECUTOR_DESERIALIZE_NAME, EXECUTOR_DESERIALIZE_SIG);
  RETURN_ERROR_IF_EXC(env);

  return Status::OK();
}

int HdfsHiveSerdeScanner::WriteFields(MemPool* pool, TupleRow* tuple_row,
                                      int num_fields, int num_tuples) {
  SCOPED_TIMER(scan_node_->materialize_tuple_timer());

  int num_tuples_materialized = 0;
  int start_idx = 0;
  TSerDeInput input;
  const char* buffer_start = field_locations_[0].start;

  // Check if we have any boundary row. If so, we need to send the whole
  // row (together with the part we read this time) to the FE for
  // a separete call.
  if (!boundary_row_.Empty()) {
    input.row_start_offsets.push_back(0);
    input.row_end_offsets.push_back(field_locations_[0].len);

    // In CopyBoundaryFields, we've already concatenated the row, and put it
    // in field_locations_[0].
    input.data = string(field_locations_[0].start, field_locations_[0].len);
    parse_status_ = WriteRowBatch(pool, input, &tuple_row, &num_tuples_materialized);
    if (!parse_status_.ok()) return 0;

    // If there was only the boundary row, return.
    if (num_tuples == 1) return num_tuples_materialized;

    buffer_start = field_locations_[1].start;
    start_idx = 1;
    input.row_start_offsets.clear();
    input.row_end_offsets.clear();
    boundary_row_.Clear();
  }

  // Send the byte buffer, which contains a set of whole rows we read this time,
  // to the FE.
  for (int i = start_idx; i < num_tuples; ++i) {
    input.row_start_offsets.push_back(field_locations_[i].start - buffer_start);
    input.row_end_offsets.push_back(row_end_locations_[i] - buffer_start);
  }

  // Pass a string (ByteBuffer on the Java side) through thrift.
  // TODO: optimize this further
  input.data = string(buffer_start, row_end_locations_[num_tuples - 1] - buffer_start);
  parse_status_ = WriteRowBatch(pool, input, &tuple_row, &num_tuples_materialized);
  if (!parse_status_.ok()) return 0;

  return num_tuples_materialized;
}

Status HdfsHiveSerdeScanner::WriteRowBatch(
    MemPool* pool, const TSerDeInput& input, TupleRow** tuple_row,
    int* num_tuples_materialized) {
  extdatasource::TRowBatch row_batch;

  // Call the FE side Java serde executor
  JNIEnv* env = getJNIEnv();
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(env));

  jbyteArray input_bytes;
  jbyteArray output_bytes;

  RETURN_IF_ERROR(SerializeThriftMsg(env, &input, &input_bytes));
  output_bytes = (jbyteArray)
                 env->CallObjectMethod(executor_, executor_deser_id_, input_bytes);

  // The output from the executor call is a RowBatch.
  TSerDeOutput output;
  RETURN_IF_ERROR(DeserializeThriftMsg(env, output_bytes, &output));
  row_batch = output.batch;

  // Materialize the contents in RowBatch to slots inside the tuple.
  RETURN_IF_ERROR(row_converter_->ResetRowBatch(&row_batch, false));
  while (row_converter_->HasNextRow()) {
    RETURN_IF_ERROR(row_converter_->MaterializeNextRow(tuple_, pool));
    (*tuple_row)->SetTuple(scan_node_->tuple_idx(), tuple_);
    if (EvalConjuncts(*tuple_row)) {
      ++*num_tuples_materialized;
      tuple_ = next_tuple(tuple_byte_size_, tuple_);
      *tuple_row = next_row(*tuple_row);
    }
  }

  return Status::OK();
}

void HdfsHiveSerdeScanner::Close() {
  // clean up JNI stuff
  if (executor_ != NULL) {
    JNIEnv* env = getJNIEnv();
    env->DeleteGlobalRef(executor_);

    Status status = JniUtil::GetJniExceptionMsg(env, "HdfsHiveSerdeScanner::Close(): ");
    if (!status.ok()) state_->LogError(status.msg());
  }

  HdfsTextScanner::Close();
}
