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

#include "exec/scannerLock.h"

ScannerLock::ScannerLock()
    :scanner_lock_("Scanner.lock", LockTracker::global()){}

ScannerLock::~ScannerLock(){}

bool ScannerLock::tryLock() {
  boost::mutex::scoped_lock lock(scanner_mutex_);
  if (scanner_lock_.try_lock()) {
    return true;
  }
  scanner_cv_.wait(lock);
  return false;
}

void ScannerLock::releaseLock(){
  scanner_lock_.unlock();
  scanner_cv_.notify_all();
}
