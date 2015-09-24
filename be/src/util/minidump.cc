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

#include "util/minidump.h"

#include <assert.h>
#include <boost/filesystem.hpp>
#include <client/linux/handler/exception_handler.h>

#include "common/logging.h"
#include "common/version.h"
#include "util/time.h"

#include "common/names.h"

namespace impala {

static bool DumpCallback(const google_breakpad::MinidumpDescriptor& descriptor,
    void* context, bool succeeded) {
  printf("Minidump written to: %s\n", descriptor.path());
  // TODO: this is probably not safe to do. Have another version of dump that
  // just directly prints using printf.
  LOG(ERROR) << "Query Log:\n" << minidump::QUERY_LOG.DumpLog();
  return succeeded;
}

void RegisterMinidump(const char* path) {
  // This needs to be a singleton.
  static bool registered = false;
  assert(!registered);
  registered = true;
  LOG(INFO) << "Registering minidump handler. Minidump directory: " << path;

  // Create the directory if it is not there. The minidump doesn't get written
  // if there is no directory.
  boost::filesystem::create_directories(path);

  google_breakpad::MinidumpDescriptor desc(path);

  // Intentionally leaked. We want this to have the lifetime of the process.
  google_breakpad::ExceptionHandler* eh =
      new google_breakpad::ExceptionHandler(desc, NULL, DumpCallback, NULL, true, -1);

  // Add the query log as part of the minidump.
  eh->RegisterAppMemory(&minidump::QUERY_LOG, sizeof(minidump::QUERY_LOG));
}

namespace minidump {
  QueryLog QUERY_LOG;
  int64_t QueryLog::LogEntry(const char* user, const char* sql) {
    int64_t idx = next_index++;
    // There is a race where the entire log rolls over in the time it takes
    // one log entry to be written. This way, two writers will simultaneously
    // get the same idx. This could result in user/sql being torn but we will
    // live with that.
    // TODO: is there some lightweight synchronization we can do in the entry?
    QueryLogEntry& entry = entries[idx % IMPALA_QUERY_LOG_NUM_ENTRIES];
    entry.idx = idx;
    entry.millis_since_epoch = UnixMillis();
    strncpy(entry.user, user, sizeof(entry.user));
    strncpy(entry.sql, sql, sizeof(entry.sql));
    return idx;
  }

  void QueryLog::QueryDone(int64_t idx) {
    QueryLogEntry& entry = entries[idx % IMPALA_QUERY_LOG_NUM_ENTRIES];
    DCHECK(entry.idx != 0);
    if (entry.idx == idx) {
      // The entry has not wrapped around, set it.
      entry.idx = -idx;
    }
  }

  string QueryLog::DumpLog() const {
    stringstream ss;
    ss << "Version: " << version << " (" << build_hash << ")" << endl;
    int64_t size = next_index - 1;
    // We don't assign the logical_idx of 0, so the first entry is at entries[1]
    int first_idx = 1;
    if (size > IMPALA_QUERY_LOG_NUM_ENTRIES) size = IMPALA_QUERY_LOG_NUM_ENTRIES;
    ss << "Queries: " << size << endl;
    for (int i = 0; i < size; ++i) {
      int idx = (first_idx + i) % IMPALA_QUERY_LOG_NUM_ENTRIES;
      const QueryLogEntry& entry = entries[idx];
      ss << "  Timestamp=" << entry.millis_since_epoch
         << " Idx=" << std::abs(entry.idx)
         << " User='" << string(entry.user, sizeof(entry.user))
         << "' SQL='" << string(entry.sql, sizeof(entry.sql))
         << "' Done=" << (entry.idx < 0 ? "true" : "false")
         << endl;
    }
    return ss.str();
  }

  QueryLog::QueryLog() : next_index(1) {
    strncpy(version, IMPALA_BUILD_VERSION, sizeof(version));
    strncpy(build_hash, IMPALA_BUILD_HASH, sizeof(build_hash));
    memset(&entries, 0, sizeof(entries));
  }
}

}

