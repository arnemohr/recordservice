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

package com.cloudera.impala.util;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.MetaStoreClientPool;
import com.cloudera.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import com.google.common.collect.Lists;

/**
 * Utility methods for interacting with the Hive Metastore.
 */
public class MetaStoreUtil {
  private static final Logger LOG = Logger.getLogger(MetaStoreUtil.class);

  // The default maximum number of partitions to fetch from the Hive metastore in one
  // RPC.
  private final static short DEFAULT_MAX_PARTITIONS_PER_RPC = 1000;

  // The maximum number of partitions to fetch from the metastore in one RPC.
  // Read from the 'hive.metastore.batch.retrieve.table.partition.max' Hive configuration
  // and defaults to DEFAULT_MAX_PARTITION_BATCH_SIZE if the value is not present in the
  // Hive configuration.
  private static short maxPartitionsPerRpc_ = DEFAULT_MAX_PARTITIONS_PER_RPC;

  static {
    // Get the value from the Hive configuration, if present.
    HiveConf hiveConf = new HiveConf(HdfsTable.class);
    String strValue = hiveConf.get(
        HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_TABLE_PARTITION_MAX.toString());
    if (strValue != null) {
      try {
        maxPartitionsPerRpc_ = Short.parseShort(strValue);
      } catch (NumberFormatException e) {
        LOG.error("Error parsing max partition batch size from HiveConfig: ", e);
      }
    }
    if (maxPartitionsPerRpc_ <= 0) {
      LOG.error(String.format("Invalid value for max partition batch size: %d. Using " +
          "default: %d", maxPartitionsPerRpc_, DEFAULT_MAX_PARTITIONS_PER_RPC));
      maxPartitionsPerRpc_ = DEFAULT_MAX_PARTITIONS_PER_RPC;
    }
  }

  /**
   * A thread pool to support multithreaded partition loading.
   */
  private static class ThreadPool {
    // Total available number of concurrent running threads in this pool.
    private static final int TOTAL_THREAD_NUM = 15;

    // Number of partitions to fetch in one HMS request.
    private static final int PARTITION_FETCH_BATCH_SIZE = 100;

    // Provide a thread pool executor service to receive a callable / runnable object.
    private static final ExecutorService EXECUTOR_SERVICE = Executors
        .newFixedThreadPool(TOTAL_THREAD_NUM);
  }

  /**
   * Given a List of partition names, fetches the matching Partitions from the HMS
   * in batches. Each batch will contain at most 'maxPartsPerRpc' partitions.
   * Returns a List containing all fetched Partitions.
   * Will throw a MetaException if any partitions in 'partNames' do not exist.
   */
  public static List<org.apache.hadoop.hive.metastore.api.Partition>
      fetchPartitionsByName(HiveMetaStoreClient client, List<String> partNames,
          String dbName, String tblName) throws MetaException, TException {
    List<org.apache.hadoop.hive.metastore.api.Partition> fetchedPartitions =
        Lists.newArrayList();
    // Fetch the partitions in batches.
    LOG.trace(String.format("Fetching %d partitions for: %s.%s using partition " +
        "batch size: %d", partNames.size(), dbName, tblName, maxPartitionsPerRpc_));
    for (int i = 0; i < partNames.size(); i += maxPartitionsPerRpc_) {
      // Get a subset of partition names to fetch.
      List<String> partsToFetch =
          partNames.subList(i, Math.min(i + maxPartitionsPerRpc_, partNames.size()));
      // Fetch these partitions from the metastore.
      fetchedPartitions.addAll(
          client.getPartitionsByNames(dbName, tblName, partsToFetch));
    }
    return fetchedPartitions;
  }

  /**
   * Identical to fetchPartitionsByName() except with retry when there is MetaException.
   */
  public static List<org.apache.hadoop.hive.metastore.api.Partition>
      fetchPartitionsByName(HiveMetaStoreClient client, List<String> partNames,
      String dbName, String tblName, int numRetries) throws TException {
    int retryAttempt = 0;
    while (true) {
      try {
        return MetaStoreUtil.fetchPartitionsByName(client, partNames, dbName, tblName);
      } catch (MetaException e) {
        // Only retry for MetaExceptions, since TExceptions could indicate a broken
        // connection which we can't recover from by retrying.
        if (retryAttempt < numRetries) {
          LOG.error(String.format("Error fetching partitions for table: %s.%s. " +
              "Retry attempt: %d/%d", dbName, tblName, retryAttempt, numRetries), e);
          ++retryAttempt;
          // TODO: Sleep for a bit?
        } else {
          throw e;
        }
      }
    }
  }
  /**
   * Multithreaded version of fetchPartitionsByName.
   */
  public static List<org.apache.hadoop.hive.metastore.api.Partition>
      fetchPartitionsByName(final MetaStoreClientPool pool, final List<String> partNames,
      final String dbName, final String tblName) throws TException {
    List<Future<List<Partition>>> loadRequests = Lists.newArrayList();

    // Issue all the requests to the thread pool.
    for (int i = 0; i < partNames.size(); i += ThreadPool.PARTITION_FETCH_BATCH_SIZE) {
      final int n = Math.min(partNames.size() - i, ThreadPool.PARTITION_FETCH_BATCH_SIZE);
      final int startIdx = i;
      loadRequests.add(ThreadPool.EXECUTOR_SERVICE.submit(
        new Callable<List<Partition>>() {
          public List<Partition> call() throws Exception {
            MetaStoreClient client = pool.getClient();
            try {
              List<String> partsToFetch = partNames.subList(startIdx, startIdx + n);
              return MetaStoreUtil.fetchPartitionsByName(
                  client.getHiveClient(), partsToFetch, dbName, tblName);
            } finally {
              client.release();
            }
          }
      }));
    }

    // Get them all and combine them to a single list.
    List<Partition> result = Lists.newArrayList();
    for (Future<List<Partition>> r: loadRequests) {
      try {
        result.addAll(r.get());
      } catch (InterruptedException e) {
        throw new TException(e);
      } catch (ExecutionException e) {
        throw new TException(e);
      }
    }
    return result;
  }
}
