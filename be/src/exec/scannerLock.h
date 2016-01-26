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

#ifndef SCANNERHELPER_H_
#define SCANNERHELPER_H_

#include <util/locks.h>
using namespace impala;

/// Wrapper class around Lock to provide additional signal functionality.
class ScannerLock {
 public:
  ScannerLock();

  virtual ~ScannerLock();

  /// Return true if the caller successfully acquired the scanner_lock_.
  /// Otherwise the caller will be blocked until the scanner_lock_ is released.
  bool tryLock();

  /// Release the scanner_lock_ and also notify the other blocked scanner threads.
  void releaseLock();

 private:
  /// Lock shared by all scanner threads. When memory is insufficient for all scanner
  /// thread running concurrently, only one scanner thread can acquire this lock.
  Lock scanner_lock_;

  /// Condition variable used to signal if the scanner_lock_ is acquired by another
  /// scanner thread. Protected by the scanner_mutex_.
  boost::condition_variable scanner_cv_;

  /// Protects scanner_cv_.
  boost::mutex scanner_mutex_;
};

#endif /* SCANNERHELPER_H_ */
