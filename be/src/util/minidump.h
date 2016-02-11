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

#ifndef IMPALA_UTIL_MINIDUMP_H
#define IMPALA_UTIL_MINIDUMP_H

#include <ostream>
#include <string>
#include <sstream>

#include <boost/cstdint.hpp>
#include "common/atomic.h"

#define IMPALA_QUERY_LOG_NUM_ENTRIES 128

namespace impala {

namespace minidump {
  // Fixed length entry for query log. Strings need not be NULL terminated.
  // TODO: expand this to contain fragments.
  struct QueryLogEntry {
    // Monotonically increasing query idx. The MSB indicates if the query is
    // done or not. Positive indices indicate in flight. Negative indicates done.
    // An idx of 0 indicates this entry is not populated.
    int64_t idx;
    // Time when query was added.
    int64_t millis_since_epoch;
    char user[10];
    char sql[256];
  };

  // Ring buffer of queries that have been issued. This structure needs to be fixed
  // size and contiguous to send as part of the mini dump. All calls to this object
  // are thread safe.
  struct QueryLog {
    char version[64];
    char build_hash[64];

    // The index to use for the next entry. If greater than
    // IMPALA_QUERY_LOG_NUM_ENTRIES, it indicates we've wrapped around.
    AtomicInt<int64_t> next_index;
    QueryLogEntry entries[IMPALA_QUERY_LOG_NUM_ENTRIES];

    // Logs the entry to the log. Returns the logical (non-wrapped) index.
    int64_t LogEntry(const char* user, const char* sql);
    int64_t LogEntry(const std::string& user, const std::string& sql) {
      return LogEntry(user.c_str(), sql.c_str());
    }

    // Called to indicate a query is done. Index should be the value returned
    // from LogEntry()
    void QueryDone(int64_t index);

    // Dumps the log as a single string. This is not guaranteed to be thread safe.
    std::string DumpLog() const;

    QueryLog();
  };

  extern QueryLog QUERY_LOG;
}

// Registers a minidump handler to generate breakpad minidumps to path, and sets the file
// size limit (bytes) for minidumps. If size_limit is negative, there will be no limit.
// See https://code.google.com/p/google-breakpad/ for more details.
void RegisterMinidump(const char* path, const int size_limit_bytes);

}

#endif
