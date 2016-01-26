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

#include "exec/scanner.h"
#include "exec/scannerLock.cc"
#include "util/debug-util.h"
#include "util/recordservice-metrics.h"

DECLARE_bool(rs_adjust_fetch_size);
DECLARE_int32(rs_scanner_min_fetch_size);
DECLARE_double(rs_fetch_size_increase_factor);
DECLARE_double(rs_fetch_size_decrease_factor);
DECLARE_double(rs_spare_capacity_correction_factor);

// MIN_FETCH_SIZE should be one at least.
const int Scanner::MIN_FETCH_SIZE = max(1, FLAGS_rs_scanner_min_fetch_size);

Scanner::Scanner(int tuple_size, bool is_record_service, int fetch_size,
    ScannerLock* scanner_lock)
    : tuple_byte_size_(tuple_size),
      spare_capacity_correction_factor_(FLAGS_rs_spare_capacity_correction_factor),
      acquired_scanner_lock_(false),
      max_fetch_size_(fetch_size),
      last_fetch_size_(min(fetch_size, MIN_FETCH_SIZE)),
      fetch_size_increase_factor_(FLAGS_rs_fetch_size_increase_factor),
      fetch_size_decrease_factor_(max(1.0, FLAGS_rs_fetch_size_decrease_factor)),
      is_dynamic_fetch_size_enabled_(is_record_service && FLAGS_rs_adjust_fetch_size),
      scanner_lock_(scanner_lock)
      {
  if (fetch_size_increase_factor_ < 0 || fetch_size_increase_factor_ > 1) {
    fetch_size_increase_factor_ = 1;
    LOG(INFO) << "fetch_size_increase_factor_ cannot < 0 or > 1, set it to 1";
  }
  if (spare_capacity_correction_factor_ < 0 || spare_capacity_correction_factor_ > 1) {
    spare_capacity_correction_factor_ = 1;
    LOG(INFO) << "spare_capacity_correction_factor_ cannot < 0 or > 1, set it to 1";
  }
}

Scanner::~Scanner() {
  ReleaseScannerLock();
}

int Scanner::GetAdjustedFetchSize() {
  int current_fetch_size = GetLastFetchSize();

  if (is_dynamic_fetch_size_enabled_) {
    do {
      // If the spare capacity is insufficient for all the scanner threads running
      // concurrently, each scanner thread will compete for a shared scanner lock. Only
      // the scanner thread which has acquired the scanner lock can fetch the new batch,
      // while others are blocked until the scanner lock is released.
      while ((current_fetch_size = GetMaxFetchSize(GetSpareCapacity())) < 1) {
        LOG(INFO) << "Compete for the scanner lock, as memory is insufficient for all "
            "scanner threads running concurrently.";
        if (TryScannerLock()) {
          break;
        }
      }

      // Use the minimum fetch size if it has acquired the scanner lock.
      // TODO: Currently, we don't care much about the thread mem_limit and the thread
      // spare capacity. Suppose the client side's batch fetching speed is very slow,
      // which causes the thread spare capacity insufficient, we need some way to detect
      // this situation and slow down the batch fetch for this scanner thread in advance.
      if (acquired_scanner_lock_) {
        current_fetch_size = max(1, min(min(max_fetch_size_, MIN_FETCH_SIZE),
            GetMaxFetchSize(GetThreadSpareCapacity())));
        last_fetch_size_ = current_fetch_size;
        break;
      }

      if (current_fetch_size > last_fetch_size_) {
        // Use fetch_size_increase_factor_ to adjust the increased fetch size.
        current_fetch_size = last_fetch_size_ + (current_fetch_size - last_fetch_size_)
            * fetch_size_increase_factor_;
      } else {
        // Use fetch_size_decrease_factor_ to adjust the decreased fetch size.
        current_fetch_size = max(1, (int)(last_fetch_size_
            - (last_fetch_size_ - current_fetch_size) * fetch_size_decrease_factor_));
      }

      last_fetch_size_ = current_fetch_size;
    } while (!HasEnoughCapacity(current_fetch_size, GetSpareCapacity()));
  }
  return current_fetch_size;
}

int Scanner::GetLastFetchSize() {
  if (!is_dynamic_fetch_size_enabled_) {
    return max_fetch_size_;
  }
  return last_fetch_size_;
}

int Scanner::GetMaxFetchSize(int64_t spare_capacity) {
  const int64_t fetch_size = spare_capacity / GetRowBatchSize();
  if (fetch_size > INT_MAX) {
    return min(max_fetch_size_, INT_MAX);
  }
  return min(max_fetch_size_, (int) (fetch_size));
}

bool Scanner::HasEnoughCapacity(int fetch_size, int64_t spare_capacity) {
  return spare_capacity >= GetRowBatchSize() * fetch_size;
}

int Scanner::GetNumActiveScanners() {
  return RecordServiceMetrics::NUM_OPEN_WORKER_SESSIONS->value();
}

int Scanner::GetRowBatchSize(){
  // Row batch size should be one at least.
  return max(1, tuple_byte_size_);
}

bool Scanner::TryScannerLock() {
  if (scanner_lock_->tryLock()) {
    acquired_scanner_lock_ = true;
  }
  return acquired_scanner_lock_;
}

void Scanner::ReleaseScannerLock() {
  if (acquired_scanner_lock_) {
    acquired_scanner_lock_ = false;
    scanner_lock_->releaseLock();
  }
}
