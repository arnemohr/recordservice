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

#ifndef SCANNER_H_
#define SCANNER_H_

#include <exec/scannerLock.h>
using namespace impala;

/// Scanner is the superclass for different Scanner classes, eg. HdfsScanner.
///
/// It encapsulates the implementation of dynamically adjusting fetch size:
///
/// 1. For each GetAdjustedFetchSize request, scanner thread will firstly get the current
///    spare capacity via the memory tracker. The current spare capacity is the minimum
///    value between the total spare capacity equally divided by the number of active
///    scanner threads and the spare capacity for this scanner thread:
///    spare_capacity = min(total_spare_capacity / num_of_active_scanner_threads,
///        scanner_spare_capacity)
///    In this way, the spare capacity can be guaranteed to be fairly shared by
///    all active scanner threads.
///
/// 2. Get the adjusted fetch size according to the current spare capacity and the row
///    batch size:
///    fetch_size = spare_capacity / row_batch_size
///    TODO: Now we are using the tuple size as the row batch size, which may not be
///    accurate actually, since tuple does not contain variable-length data, eg. strings
///    and nested arrays.
///
/// 3. If the adjusted fetch size is less than 1, which means the spare capacity is
///    insufficient for all the scanner threads running concurrently, each scanner thread
///    will compete for a shared scanner lock. Only the scanner thread which has acquired
///    the scanner lock can fetch the new batch, while the other scanner threads are
///    blocked until the scanner lock is released.
///    TODO: A better way to suspend the scanner threads until memory is freed up. For
///    example, use a coordinator to control the number of active scanner threads, allow
///    more than one thread to fetch the new batch when memory is insufficient and etc.
///
/// 4. Use some configurable factors to correct the current spare capacity, and adjust
///    the increased / decreased fetch size. For example:
///    If the current fetch size is larger than the last fetch size, we will use
///    rs_fetch_size_increase_factor which is > 0 and <= 1, to adjust the increased fetch
///    size. And if the current fetch size is slower than the last fetch size, we will
///    use rs_fetch_size_decrease_factor which is >= 1, to adjust the decreased fetch
///    size.
///    With these two factors, the fetch size is increased slowly, and decreased quickly.
///    In theory, it should be fine to just use the original fetch size without these two
///    factors, while since the memory estimation may not be accurate, slowly increase
///    and quickly decrease can help to avoid resource contention.

class Scanner {
 public:
  Scanner(int tuple_size, bool is_record_service, int fetch_size, ScannerLock* sc);

  virtual ~Scanner();

  /// Reduce the maximum fetch size if fetch_size is smaller than the current maximum
  /// fetch size.
  void ReduceMaxFetchSize(int fetch_size);

 protected:
  /// Fixed size of each top-level tuple, in bytes.
  int tuple_byte_size_;

  /// Correction factor to adjust the spare capacity, must > 0 and <= 1.
  /// The larger this value, the smaller the spare capacity.
  float spare_capacity_correction_factor_;

  /// Flag to show if the scanner lock is acquired by this scanner thread.
  /// If it is true, this scanner thread is the only one that is allowed to fetch the new
  /// batch, while others are blocked and waiting for the release of the scanner lock.
  /// Also it is a sign that the spare capacity is insufficient for all the scanner
  /// threads running concurrently.
  bool acquired_scanner_lock_;

  /// Return the adjusted fetch size according to the current spare capacity and the
  /// row batch size. If memory is insufficient, only one scanner thread can fetch a
  /// new row batch with MIN_FETCH_SIZE, while others will blocked until the memory is
  /// freed up. Use the user customized fetch size if adjust_fetch_size_ is false.
  int GetAdjustedFetchSize();

  /// Return the spare capacity according to the spare capacity and the number of active
  /// scanners in real time. It will be the minimum value between the total spare
  /// capacity equally divided by all the active scanners and the spare capacity only for
  /// this scanner thread.
  virtual int64_t GetSpareCapacity() = 0;

  /// Return the spare capacity only for this scanner thread. It will be less than or
  /// equals to the total spare capacity for the process.
  virtual int64_t GetThreadSpareCapacity() = 0;

  /// Return the number of active scanner threads.
  int GetNumActiveScanners();

  /// Release the scanner lock if the lock is acquired by this thread, and also wake up
  /// all the other scanner threads that are waiting for the lock.
  void ReleaseScannerLock();

 private:
  /// The minimum fetch size when the memory is only sufficient for the current scanner
  /// thread.
  static const int MIN_FETCH_SIZE;

  /// The maximum fetch size in one row batch. It initially uses the value set by client,
  /// and can be changed via calling ReduceMaxFetchSize function.
  int max_fetch_size_;

  /// Fetch size in last time, and will be a reference for the adjusted fetch size.
  int last_fetch_size_;

  /// Correction factor to adjust the increased fetch size, must > 0 and <= 1.
  /// The smaller this value, the smaller the increased fetch size.
  float fetch_size_increase_factor_;

  /// Correction factor to adjust the decreased fetch size, must >= 1.
  /// The larger this value, the larger the decreased fetch size.
  float fetch_size_decrease_factor_;

  /// Flag to show if the fetch size should be adjusted. Will be set to true only when
  /// it is a RecordService request and FLAGS_rs_adjust_fetch_size is true.
  bool is_dynamic_fetch_size_enabled_;

  /// A lock shared by all scanner threads. NOT owned here. Will be used when the memory
  /// is insufficient for all the scanner threads running concurrently.
  ScannerLock* scanner_lock_;

  /// Return the fetch size for last time. If not need to adjust fetch size, it will
  /// return the fetch size set by client.
  int GetLastFetchSize();

  /// Return the maximum possible fetch size according to the spare_capacity and row
  /// batch size.
  int GetMaxFetchSize(int64_t spare_capacity);

  /// Return true if the spare_capacity is enough for the fetch_size.
  bool HasEnoughCapacity(int fetch_size, int64_t spare_capacity);

  /// Return the estimation of row batch size. Now we are using the tuple size.
  /// TODO: We need a more accurate estimation for the memory consumption of a row batch,
  /// since tuple does not contain variable-length data, eg. strings and nested arrays.
  int GetRowBatchSize();

  /// Return true if the current scanner thread successfully acquired the scanner_lock_.
  /// Otherwise it will be blocked until the scanner_lock_ is released.
  bool TryScannerLock();
};

#endif
