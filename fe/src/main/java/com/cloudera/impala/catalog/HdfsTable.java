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

package com.cloudera.impala.catalog;

import static com.codahale.metrics.MetricRegistry.name;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.analysis.NullLiteral;
import com.cloudera.impala.analysis.NumericLiteral;
import com.cloudera.impala.analysis.PartitionKeyValue;
import com.cloudera.impala.catalog.HdfsPartition.FileDescriptor;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.FileSystemUtil;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.common.PrintUtils;
import com.cloudera.impala.thrift.ImpalaInternalServiceConstants;
import com.cloudera.impala.thrift.TAccessLevel;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TColumn;
import com.cloudera.impala.thrift.THdfsFileBlock;
import com.cloudera.impala.thrift.THdfsPartition;
import com.cloudera.impala.thrift.THdfsTable;
import com.cloudera.impala.thrift.TNetworkAddress;
import com.cloudera.impala.thrift.TPartitionKeyValue;
import com.cloudera.impala.thrift.TResultRow;
import com.cloudera.impala.thrift.TResultSet;
import com.cloudera.impala.thrift.TResultSetMetadata;
import com.cloudera.impala.thrift.TTable;
import com.cloudera.impala.thrift.TTableDescriptor;
import com.cloudera.impala.thrift.TTableType;
import com.cloudera.impala.util.AvroSchemaConverter;
import com.cloudera.impala.util.AvroSchemaParser;
import com.cloudera.impala.util.FsPermissionChecker;
import com.cloudera.impala.util.HdfsCachingUtil;
import com.cloudera.impala.util.ListMap;
import com.cloudera.impala.util.LoadMetadataUtil;
import com.cloudera.impala.util.LoadMetadataUtil.FileBlocksInfo;
import com.cloudera.impala.util.LoadMetadataUtil.FsKey;
import com.cloudera.impala.util.MetaStoreUtil;
import com.cloudera.impala.util.Metrics;
import com.cloudera.impala.util.TAccessLevelUtil;
import com.cloudera.impala.util.TResultRowBuilder;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Internal representation of table-related metadata of a file-resident table on a
 * Hadoop filesystem.  The table data can be accessed through libHDFS (which is more of
 * an abstraction over Hadoop's FileSystem class rather than DFS specifically).  A
 * partitioned table can even span multiple filesystems.
 *
 * Owned by Catalog instance.
 * The partition keys constitute the clustering columns.
 *
 * This class is not thread-safe due to the static counter variable inside HdfsPartition.
 * Also not thread safe because of possibility of concurrent modifications to the list of
 * partitions in methods addPartition and dropPartition.
 */
public class HdfsTable extends Table {
  // hive's default value for table property 'serialization.null.format'
  private static final String DEFAULT_NULL_COLUMN_VALUE = "\\N";

  // Number of times to retry fetching the partitions from the HMS should an error occur.
  private static final int NUM_PARTITION_FETCH_RETRIES = 5;

  // string to indicate NULL. set in load() from table properties
  private String nullColumnValue_;

  // hive uses this string for NULL partition keys. Set in load().
  private String nullPartitionKeyValue_;

  // Avro schema of this table if this is an Avro table, otherwise null. Set in load().
  private String avroSchema_ = null;

  // True if this table's metadata is marked as cached. Does not necessarily mean the
  // data is cached or that all/any partitions are cached.
  private boolean isMarkedCached_ = false;

  private final List<HdfsPartition> partitions_; // these are only non-empty partitions

  // Array of sorted maps storing the association between partition values and
  // partition ids. There is one sorted map per partition key.
  private final ArrayList<TreeMap<LiteralExpr, HashSet<Long>>> partitionValuesMap_ =
      Lists.newArrayList();

  // Array of partition id sets that correspond to partitions with null values
  // in the partition keys; one set per partition key.
  private final ArrayList<HashSet<Long>> nullPartitionIds_ = Lists.newArrayList();

  // Map of partition ids to HdfsPartitions. Used for speeding up partition
  // pruning.
  private final HashMap<Long, HdfsPartition> partitionMap_ = Maps.newHashMap();

  // Store all the partition ids of an HdfsTable.
  private final HashSet<Long> partitionIds_ = Sets.newHashSet();

  // Flag to indicate if the HdfsTable has the partition metadata populated.
  private boolean hasPartitionMd_ = false;

  // Bi-directional map between an integer index and a unique datanode
  // TNetworkAddresses, each of which contains blocks of 1 or more
  // files in this table. The network addresses are stored using IP
  // address as the host name. Each FileBlock specifies a list of
  // indices within this hostIndex_ to specify which nodes contain
  // replicas of the block.
  private final ListMap<TNetworkAddress> hostIndex_ = new ListMap<TNetworkAddress>();

  // Map of parent directory (partition location) to list of files (FileDescriptors)
  // under that directory. Used to look up/index all files in the table.
  private final Map<String, List<FileDescriptor>> fileDescMap_ = Maps.newHashMap();

  // Total number of Hdfs files in this table. Set in load().
  private long numHdfsFiles_;

  // Sum of sizes of all Hdfs files in this table. Set in load().
  private long totalHdfsBytes_;

  // True if the table's partitions are located on more than one filesystem.
  // Possibly be updated by multiple threads when loading block locations.
  private volatile boolean multipleFileSystems_ = false;

  // Base Hdfs directory where files of this table are stored.
  // For unpartitioned tables it is simply the path where all files live.
  // For partitioned tables it is the root directory
  // under which partition dirs are placed.
  protected String hdfsBaseDir_;

  // List of FieldSchemas that correspond to the non-partition columns. Used when
  // describing this table and its partitions to the HMS (e.g. as part of an alter table
  // operation), when only non-partition columns are required.
  private final List<FieldSchema> nonPartFieldSchemas_ = Lists.newArrayList();

  private final static Logger LOG = LoggerFactory.getLogger(HdfsTable.class);

  // TODO: Keep the naming scheme consistent with BE metrics
  // LOAD_HDFSTABLE_METADATA.db_name.tbl_name
  private final String loadTblMetadataTimerName_;

  // LIST_PARTITIONS.db_name.tbl_name
  private final String listPartitionsTimerName_;

  // LOAD_BLOCK_LOCATION.db_name.tbl_name
  private final String loadBlockLocationTimerName_;

  // LOAD_BLOCK_LOCATION_PER_PARTITION.db_name.tbl_name
  private final String loadBlockLocationPerPartitionTimerName_;

  // LOAD_VOLUME_IDS.db_name.tbl_name
  private final String loadVolumeIdsTimerName_;

  // LOAD_VOLUME_IDS_PER_PARTITION.db_name.tbl_name
  private final String loadVolumeIdsPerPartition_;

  // Decide if using single thread when loading metadata for partitions.
  private static final String ENABLE_MULTITHREADED_METADATA_LOADING_CONF =
      "recordservice.server.enableMultithreadedMetadataLoading";
  private final boolean enableMultithreadedMetadataLoading_;

  public boolean spansMultipleFileSystems() { return multipleFileSystems_; }

  public HdfsTable(TableId id, org.apache.hadoop.hive.metastore.api.Table msTbl,
      Db db, String name, String owner) {
    super(id, msTbl, db, name, owner);
    this.partitions_ = Lists.newArrayList();
    this.loadBlockLocationTimerName_ = name("LOAD_BLOCK_LOCATION", getFullName());
    this.loadVolumeIdsTimerName_ = name("LOAD_VOLUME_IDS", getFullName());
    this.loadTblMetadataTimerName_ = name("LOAD_HDFSTABLE_METADATA", getFullName());
    this.listPartitionsTimerName_ = name("LIST_PARTITIONS", getFullName());
    this.loadBlockLocationPerPartitionTimerName_ = name(
        "LOAD_BLOCK_LOCATION_PER_PARTITION", getFullName());
    this.loadVolumeIdsPerPartition_ = name("LOAD_VOLUME_IDS_PER_PARTITION",
        getFullName());

    enableMultithreadedMetadataLoading_ = LoadMetadataUtil.getConf().getBoolean(
        ENABLE_MULTITHREADED_METADATA_LOADING_CONF, true);
    LOG.debug("Loading table " + name + (enableMultithreadedMetadataLoading_ ?
        " with multiple threads." : " with single thread."));
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() { return TCatalogObjectType.TABLE; }
  public List<HdfsPartition> getPartitions() {
    return new ArrayList<HdfsPartition>(partitions_);
  }
  public boolean isMarkedCached() { return isMarkedCached_; }

  public HashMap<Long, HdfsPartition> getPartitionMap() { return partitionMap_; }
  public HashSet<Long> getNullPartitionIds(int i) { return nullPartitionIds_.get(i); }
  public HashSet<Long> getPartitionIds() { return partitionIds_; }
  public TreeMap<LiteralExpr, HashSet<Long>> getPartitionValueMap(int i) {
    return partitionValuesMap_.get(i);
  }

  /**
   * Returns the value Hive is configured to use for NULL partition key values.
   * Set during load.
   */
  public String getNullPartitionKeyValue() { return nullPartitionKeyValue_; }
  public String getNullColumnValue() { return nullColumnValue_; }

  /*
   * Returns the storage location (HDFS path) of this table.
   */
  public String getLocation() { return super.getMetaStoreTable().getSd().getLocation(); }

  List<FieldSchema> getNonPartitionFieldSchemas() { return nonPartFieldSchemas_; }

  // True if Impala has HDFS write permissions on the hdfsBaseDir (for an unpartitioned
  // table) or if Impala has write permissions on all partition directories (for
  // a partitioned table).
  public boolean hasWriteAccess() {
    return TAccessLevelUtil.impliesWriteAccess(accessLevel_);
  }

  /**
   * Returns the first location (HDFS path) that Impala does not have WRITE access
   * to, or an null if none is found. For an unpartitioned table, this just
   * checks the hdfsBaseDir. For a partitioned table it checks all partition directories.
   */
  public String getFirstLocationWithoutWriteAccess() {
    if (getMetaStoreTable() == null) return null;

    if (getMetaStoreTable().getPartitionKeysSize() == 0) {
      if (!TAccessLevelUtil.impliesWriteAccess(accessLevel_)) {
        return hdfsBaseDir_;
      }
    } else {
      for (HdfsPartition partition: partitions_) {
        if (!TAccessLevelUtil.impliesWriteAccess(partition.getAccessLevel())) {
          return partition.getLocation();
        }
      }
    }
    return null;
  }

  /**
   * Gets the HdfsPartition matching the given partition spec. Returns null if no match
   * was found.
   */
  public HdfsPartition getPartition(List<PartitionKeyValue> partitionSpec) {
    List<TPartitionKeyValue> partitionKeyValues = Lists.newArrayList();
    for (PartitionKeyValue kv: partitionSpec) {
      String value = PartitionKeyValue.getPartitionKeyValueString(
          kv.getLiteralValue(), getNullPartitionKeyValue());
      partitionKeyValues.add(new TPartitionKeyValue(kv.getColName(), value));
    }
    return getPartitionFromThriftPartitionSpec(partitionKeyValues);
  }

  /**
   * Gets the HdfsPartition matching the Thrift version of the partition spec.
   * Returns null if no match was found.
   */
  public HdfsPartition getPartitionFromThriftPartitionSpec(
      List<TPartitionKeyValue> partitionSpec) {
    // First, build a list of the partition values to search for in the same order they
    // are defined in the table.
    List<String> targetValues = Lists.newArrayList();
    Set<String> keys = Sets.newHashSet();
    for (FieldSchema fs: getMetaStoreTable().getPartitionKeys()) {
      for (TPartitionKeyValue kv: partitionSpec) {
        if (fs.getName().toLowerCase().equals(kv.getName().toLowerCase())) {
          targetValues.add(kv.getValue().toLowerCase());
          // Same key was specified twice
          if (!keys.add(kv.getName().toLowerCase())) {
            return null;
          }
        }
      }
    }

    // Make sure the number of values match up and that some values were found.
    if (targetValues.size() == 0 ||
        (targetValues.size() != getMetaStoreTable().getPartitionKeysSize())) {
      return null;
    }

    // Now search through all the partitions and check if their partition key values match
    // the values being searched for.
    for (HdfsPartition partition: getPartitions()) {
      if (partition.getId() == ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID) {
        continue;
      }
      List<LiteralExpr> partitionValues = partition.getPartitionValues();
      Preconditions.checkState(partitionValues.size() == targetValues.size());
      boolean matchFound = true;
      for (int i = 0; i < targetValues.size(); ++i) {
        String value;
        if (partitionValues.get(i) instanceof NullLiteral) {
          value = getNullPartitionKeyValue();
        } else {
          value = partitionValues.get(i).getStringValue();
          Preconditions.checkNotNull(value);
          // See IMPALA-252: we deliberately map empty strings on to
          // NULL when they're in partition columns. This is for
          // backwards compatibility with Hive, and is clearly broken.
          if (value.isEmpty()) value = getNullPartitionKeyValue();
        }
        if (!targetValues.get(i).equals(value.toLowerCase())) {
          matchFound = false;
          break;
        }
      }
      if (matchFound) {
        return partition;
      }
    }
    return null;
  }

  /**
   * Create columns corresponding to fieldSchemas. Throws a TableLoadingException if the
   * metadata is incompatible with what we support.
   */
  private void addColumnsFromFieldSchemas(List<FieldSchema> fieldSchemas)
      throws TableLoadingException {
    int pos = colsByPos_.size();
    for (FieldSchema s: fieldSchemas) {
      Type type = parseColumnType(s);
      // Check if we support partitioning on columns of such a type.
      if (pos < numClusteringCols_ && !type.supportsTablePartitioning()) {
        throw new TableLoadingException(
            String.format("Failed to load metadata for table '%s' because of " +
                "unsupported partition-column type '%s' in partition column '%s'",
                getFullName(), type.toString(), s.getName()));
      }

      Column col = new Column(s.getName(), type, s.getComment(), pos);
      addColumn(col);
      ++pos;
    }
  }

  /**
   * Populate the partition metadata of an HdfsTable.
   */
  private void populatePartitionMd() {
    if (hasPartitionMd_) return;
    for (HdfsPartition partition: partitions_) {
      updatePartitionMdAndColStats(partition);
    }
    hasPartitionMd_ = true;
  }

  /**
   * Clear the partition metadata of an HdfsTable including column stats.
   */
  private void resetPartitionMd() {
    partitionIds_.clear();
    partitionMap_.clear();
    partitionValuesMap_.clear();
    nullPartitionIds_.clear();
    // Initialize partitionValuesMap_ and nullPartitionIds_. Also reset column stats.
    for (int i = 0; i < numClusteringCols_; ++i) {
      getColumns().get(i).getStats().setNumNulls(0);
      getColumns().get(i).getStats().setNumDistinctValues(0);
      partitionValuesMap_.add(Maps.<LiteralExpr, HashSet<Long>>newTreeMap());
      nullPartitionIds_.add(Sets.<Long>newHashSet());
    }
    hasPartitionMd_ = false;
  }

  /**
   * Create HdfsPartition objects corresponding to 'partitions'.
   *
   * If there are no partitions in the Hive metadata, a single partition is added with no
   * partition keys.
   *
   * For files that have not been changed, reuses file descriptors from oldFileDescMap.
   *
   * TODO: If any partition fails to load, the entire table will fail to load. Instead,
   * we should consider skipping partitions that cannot be loaded and raise a warning
   * whenever the table is accessed.
   */
  private void loadPartitions(
      List<org.apache.hadoop.hive.metastore.api.Partition> msPartitions,
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      Map<String, List<FileDescriptor>> oldFileDescMap) throws IOException,
      CatalogException, InterruptedException, ExecutionException {
    resetPartitionMd();
    partitions_.clear();
    hdfsBaseDir_ = msTbl.getSd().getLocation();

    // Start timer for loadBlockMetadata.
    final Timer.Context loadBlockLocationTimer = Metrics.INSTANCE
        .getTimerCtx(loadBlockLocationTimerName_);

    // Map of filesystem to the file blocks for new/modified FileDescriptors. Blocks in
    // this map will have their disk volume IDs information (re)loaded. This is used to
    // speed up the incremental refresh of a table's metadata by skipping unmodified,
    // previously loaded blocks.
    Map<FsKey, FileBlocksInfo> blocksToLoad = Maps.newHashMap();

    // INSERT statements need to refer to this if they try to write to new partitions
    // Scans don't refer to this because by definition all partitions they refer to
    // exist.
    addDefaultPartition(msTbl.getSd());

    // We silently ignore cache directives that no longer exist in HDFS, and remove
    // non-existing cache directives from the parameters.
    isMarkedCached_ = HdfsCachingUtil.validateCacheParams(msTbl.getParameters());

    if (msTbl.getPartitionKeysSize() == 0) {
      Preconditions.checkArgument(msPartitions == null || msPartitions.isEmpty());
      // This table has no partition key, which means it has no declared partitions.
      // We model partitions slightly differently to Hive - every file must exist in a
      // partition, so add a single partition with no keys which will get all the
      // files in the table's root directory.
      HdfsPartition part = createPartition(msTbl.getSd(), null, oldFileDescMap,
          blocksToLoad);
      addPartition(part);
      if (isMarkedCached_) part.markCached();
      Path location = new Path(hdfsBaseDir_);
      FileSystem fs = location.getFileSystem(LoadMetadataUtil.getConf());
      if (fs.exists(location)) {
        accessLevel_ = getAvailableAccessLevel(fs, location);
      }
    } else if (!enableMultithreadedMetadataLoading_) {
      // Single thread case.
      blocksToLoad = loadBlockMdForPartitions(msPartitions, 0, msPartitions.size(),
          oldFileDescMap);
    } else {
      // Multiple threads case.
      int partitionsPerThread = msPartitions.size() / ThreadPool.THREAD_NUM_PER_REQUEST;
      // When there are fewer partitions than threads, assign 1 per thread until there are
      // no partitions left.
      if (partitionsPerThread == 0) partitionsPerThread = 1;
      int numUnassignedPartitions = msPartitions.size();

      List<Future<Map<FsKey, FileBlocksInfo>>> futureList =
          new LinkedList<Future<Map<FsKey, FileBlocksInfo>>>();

      // Each thread will be assigned with the same number of partitions except when
      // partition number cannot be divisible by THREAD_NUM_PER_REQUEST, or when there are
      // less partition number than the thread number.
      for (int i = 0; i < ThreadPool.THREAD_NUM_PER_REQUEST
          && numUnassignedPartitions > 0; ++i) {
        // When the partition number cannot be divisible by THREAD_NUM_PER_REQUEST, the
        // last thread will be assigned with the partitions left.
        if (i == ThreadPool.THREAD_NUM_PER_REQUEST - 1) {
          partitionsPerThread = numUnassignedPartitions;
        }

        // Submit callable task to thread pool.
        futureList.add(ThreadPool.EXECUTOR_SERVICE.submit(new BlockMdLoader(msPartitions,
            msPartitions.size() - numUnassignedPartitions, partitionsPerThread,
            oldFileDescMap)));

        numUnassignedPartitions -= partitionsPerThread;
      }

      // Block until all tasks complete and copy map of block info into blocksToLoad
      for (Future<Map<FsKey, FileBlocksInfo>> future: futureList) {
        blocksToLoad.putAll(future.get());
      }
    }

    // Stop timer for loadBlockLocation.
    long timeSpent = loadBlockLocationTimer.stop() / Metrics.NANOTOMILLISEC;
    LOG.info("{}: {}millisec", loadBlockLocationTimerName_, timeSpent);

    // Update histogram for loadBlockLocationPerPartition.
    if (msPartitions.size() > 0) {
      Metrics.INSTANCE.getHistogram(loadBlockLocationPerPartitionTimerName_).update(
          timeSpent / msPartitions.size());
      LOG.info("{}: {}millisec", loadBlockLocationPerPartitionTimerName_, timeSpent
          / msPartitions.size());
    }

    // Start timer for loadVolumeIds.
    final Timer.Context loadVolumeIdsTimer = Metrics.INSTANCE
        .getTimerCtx(loadVolumeIdsTimerName_);

    LoadMetadataUtil.loadDiskIds(getFullName(), hostIndex_.size(), blocksToLoad);

    // Stop timer for loadVolumeIds.
    timeSpent = loadVolumeIdsTimer.stop() / Metrics.NANOTOMILLISEC;
    LOG.info("{}: {}millisec", loadVolumeIdsTimerName_, timeSpent);

    // Update histogram for loadVolumeIdsPerPartition.
    if (msPartitions.size() > 0) {
      Metrics.INSTANCE.getHistogram(loadVolumeIdsPerPartition_).update(timeSpent
          / msPartitions.size());
      LOG.info("{}: {}millisec", loadVolumeIdsPerPartition_, timeSpent
          / msPartitions.size());
    }
  }

  /**
   * Load block locations with partitions. It only loads 'len' partitions from 'start'
   * index in the 'partitionList', and return map of filesystem to the file blocks
   * for new or modified FileDescriptors.
   * TODO: Make sure it is thread safe in multiple thread mode.
   * TODO: Use ImmutableMap for oldFileDescMap and ImmutableList for partitionList.
   */
  private Map<FsKey, FileBlocksInfo> loadBlockMdForPartitions(
      List<org.apache.hadoop.hive.metastore.api.Partition> partitionList, int start,
      int len, Map<String, List<FileDescriptor>> oldFileDescMap) throws CatalogException {
    Map<FsKey, FileBlocksInfo> blocksToLoad = Maps.newHashMap();
    for (int i = 0; i < len; ++i) {
      org.apache.hadoop.hive.metastore.api.Partition msPartition =
          partitionList.get(start + i);
      HdfsPartition partition = createPartition(msPartition.getSd(), msPartition,
          oldFileDescMap, blocksToLoad);
      addPartition(partition);

      // If the partition is null, its HDFS path does not exist, and it was not added to
      // this table's partition list. Skip the partition.
      if (partition == null) {
        continue;
      }

      if (msPartition.getParameters() != null) {
        partition.setNumRows(getRowCount(msPartition.getParameters()));
      }
      if (!TAccessLevelUtil.impliesWriteAccess(partition.getAccessLevel())) {
        // TODO: READ_ONLY isn't exactly correct because the it's possible the
        // partition does not have READ permissions either. When we start checking
        // whether we can READ from a table, this should be updated to set the
        // table's access level to the "lowest" effective level across all
        // partitions. That is, if one partition has READ_ONLY and another has
        // WRITE_ONLY the table's access level should be NONE.
        accessLevel_ = TAccessLevel.READ_ONLY;
      }
    }

    return blocksToLoad;
  }

  /**
   * A thread pool to support multithreading process for loading block metadata. The total
   * thread number is defined to limit the number of concurrent running threads.
   */
  private static class ThreadPool {
    // Total available number of concurrent running threads in this pool.
    private static final int TOTAL_THREAD_NUM = 15;

    // Number of threads for each load partitions request.
    private static final int THREAD_NUM_PER_REQUEST = 10;

    // Provide a thread pool executor service to receive a callable / runnable object.
    private static final ExecutorService EXECUTOR_SERVICE = Executors
        .newFixedThreadPool(TOTAL_THREAD_NUM);
  }

  /**
   * Support to load block location in multiple threads, and return map of filesystem to
   * the file blocks for new or modified FileDescriptors.
   */
  private class BlockMdLoader implements Callable<Map<FsKey, FileBlocksInfo>> {
    private final List<org.apache.hadoop.hive.metastore.api.Partition> partitionList;
    // Start index in partitionList it will process.
    private final int start;
    // Length of partitions it will process.
    private final int len;
    private final Map<String, List<FileDescriptor>> oldFileDescMap;

    public BlockMdLoader(
        List<org.apache.hadoop.hive.metastore.api.Partition> partitionList, int start,
        int len, Map<String, List<FileDescriptor>> oldFileDescMap) {
      this.partitionList = partitionList;
      this.start = start;
      this.len = len;
      this.oldFileDescMap = oldFileDescMap;
    }

    @Override
    public Map<FsKey, FileBlocksInfo> call() throws CatalogException {
      return loadBlockMdForPartitions(partitionList, start, len, oldFileDescMap);
    }
  }

  /**
   * Gets the AccessLevel that is available for Impala for this table based on the
   * permissions Impala has on the given path. If the path does not exist, recurses up the
   * path until a existing parent directory is found, and inherit access permissions from
   * that.
   */
  private TAccessLevel getAvailableAccessLevel(FileSystem fs, Path location)
      throws IOException {
    FsPermissionChecker permissionChecker = FsPermissionChecker.getInstance();
    while (location != null) {
      if (fs.exists(location)) {
        FsPermissionChecker.Permissions perms =
            permissionChecker.getPermissions(fs, location);
        if (perms.canReadAndWrite()) {
          return TAccessLevel.READ_WRITE;
        } else if (perms.canRead()) {
          return TAccessLevel.READ_ONLY;
        } else if (perms.canWrite()) {
          return TAccessLevel.WRITE_ONLY;
        }
        return TAccessLevel.NONE;
      }
      location = location.getParent();
    }
    // Should never get here.
    Preconditions.checkNotNull(location, "Error: no path ancestor exists");
    return TAccessLevel.NONE;
  }

  /**
   * Creates a new HdfsPartition object to be added to HdfsTable's partition list.
   * Partitions may be empty, or may not even exist in the filesystem (a partition's
   * location may have been changed to a new path that is about to be created by an
   * INSERT). Also loads the block metadata for this partition.
   * Returns new partition if successful or null if none was added.
   * Separated from addPartition to reduce the number of operations done while holding
   * the lock on HdfsTable.
   *
   *  @throws CatalogException
   *    if the supplied storage descriptor contains metadata that Impala can't
   *    understand.
   */
  public HdfsPartition createPartition(StorageDescriptor storageDescriptor,
      org.apache.hadoop.hive.metastore.api.Partition msPartition)
      throws CatalogException {
    Map<FsKey, FileBlocksInfo> blocksToLoad = Maps.newHashMap();
    HdfsPartition hdfsPartition = createPartition(storageDescriptor, msPartition,
        fileDescMap_, blocksToLoad);
    LoadMetadataUtil.loadDiskIds(getFullName(), hostIndex_.size(), blocksToLoad);
    return hdfsPartition;
  }

  /**
   * Creates a new HdfsPartition object to be added to the internal partition list.
   * Populates with file format information and file locations. Partitions may be empty,
   * or may not even exist on the filesystem (a partition's location may have been
   * changed to a new path that is about to be created by an INSERT). For unchanged
   * files (indicated by unchanged mtime), reuses the FileDescriptor from the
   * oldFileDescMap. The one exception is if the partition is marked as cached
   * in which case the block metadata cannot be reused. Otherwise, creates a new
   * FileDescriptor for each modified or new file and adds it to newFileDescMap.
   * Both old and newFileDescMap are Maps of parent directory (partition location)
   * to list of files (FileDescriptors) under that directory.
   * Returns new partition if successful or null if none was added.
   * Separated from addPartition to reduce the number of operations done
   * while holding the lock on the hdfs table.
   *
   * Must be thread safe.
   *
   *  @throws CatalogException
   *    if the supplied storage descriptor contains metadata that Impala can't
   *    understand.
   */
  private HdfsPartition createPartition(StorageDescriptor storageDescriptor,
      org.apache.hadoop.hive.metastore.api.Partition msPartition,
      Map<String, List<FileDescriptor>> oldFileDescMap,
      Map<FsKey, FileBlocksInfo> perFsFileBlocks)
      throws CatalogException {
    HdfsStorageDescriptor fileFormatDescriptor =
        HdfsStorageDescriptor.fromStorageDescriptor(name_, storageDescriptor);
    Path partDirPath = new Path(storageDescriptor.getLocation());

    // If the partition is marked as cached, the block location metadata must be
    // reloaded, even if the file times have not changed.
    boolean isMarkedCached = isMarkedCached_;
    List<LiteralExpr> keyValues = Lists.newArrayList();
    if (msPartition != null) {
      isMarkedCached = HdfsCachingUtil.validateCacheParams(msPartition.getParameters());
      // Load key values
      for (String partitionKey: msPartition.getValues()) {
        Type type = getColumns().get(keyValues.size()).getType();
        // Deal with Hive's special NULL partition key.
        if (partitionKey.equals(nullPartitionKeyValue_)) {
          keyValues.add(NullLiteral.create(type));
        } else {
          try {
            keyValues.add(LiteralExpr.create(partitionKey, type));
          } catch (Exception ex) {
            LOG.warn("Failed to create literal expression of type: " + type, ex);
            throw new CatalogException("Invalid partition key value of type: " + type,
                ex);
          }
        }
      }
      try {
        Expr.analyze(keyValues, null);
      } catch (AnalysisException e) {
        // should never happen
        throw new IllegalStateException(e);
      }
    }
    try {
      // Each partition could reside on a different filesystem.
      FileSystem fs = partDirPath.getFileSystem(LoadMetadataUtil.getConf());
      if (!FileSystemUtil.isPathOnFileSystem(new Path(getLocation()), fs)) {
        multipleFileSystems_ = true;
      }
      List<FileDescriptor> fileDescriptors = Lists.newArrayList();
      if (fs.exists(partDirPath)) {
        // FileSystem does not have an API that takes in a timestamp and returns a list
        // of files that has been added/changed since. Therefore, we are calling
        // fs.listStatus() to list all the files.
        fileDescriptors = LoadMetadataUtil.loadFileDescriptors(fs, partDirPath,
            oldFileDescMap, fileFormatDescriptor.getFileFormat(), perFsFileBlocks,
            isMarkedCached, name_, hostIndex_, fileDescMap_);
        addNumHdfsFiles(fileDescriptors.size());
      }
      HdfsPartition partition = new HdfsPartition(this, msPartition, keyValues,
          fileFormatDescriptor, fileDescriptors,
          getAvailableAccessLevel(fs, partDirPath));
      partition.checkWellFormed();
      return partition;
    } catch (Exception e) {
      throw new CatalogException("Failed to create partition: ", e);
    }
  }

  /**
   * Adds the partition to the HdfsTable.
   * Threadsafe.
   */
  void addPartition(HdfsPartition partition) {
    synchronized (partitions_) {
      if (partitions_.contains(partition)) return;
      partitions_.add(partition);
      totalHdfsBytes_ += partition.getSize();
      updatePartitionMdAndColStats(partition);
    }
  }

  /**
   * Updates the HdfsTable's partition metadata, i.e. adds the id to the HdfsTable and
   * populates structures used for speeding up partition pruning/lookup. Also updates
   * column stats.
   */
  private void updatePartitionMdAndColStats(HdfsPartition partition) {
    if (partition.getPartitionValues().size() != numClusteringCols_) return;

    partitionIds_.add(partition.getId());
    partitionMap_.put(partition.getId(), partition);
    for (int i = 0; i < partition.getPartitionValues().size(); ++i) {
      ColumnStats stats = getColumns().get(i).getStats();
      LiteralExpr literal = partition.getPartitionValues().get(i);
      // Store partitions with null partition values separately
      if (literal instanceof NullLiteral) {
        stats.setNumNulls(stats.getNumNulls() + 1);
        if (nullPartitionIds_.get(i).isEmpty()) {
          stats.setNumDistinctValues(stats.getNumDistinctValues() + 1);
        }
        nullPartitionIds_.get(i).add(partition.getId());
        continue;
      }
      HashSet<Long> partitionIds = partitionValuesMap_.get(i).get(literal);
      if (partitionIds == null) {
        partitionIds = Sets.newHashSet();
        partitionValuesMap_.get(i).put(literal, partitionIds);
        stats.setNumDistinctValues(stats.getNumDistinctValues() + 1);
      }
      partitionIds.add(partition.getId());
    }
  }

  /**
   * Drops the partition having the given partition spec from HdfsTable. Cleans up its
   * metadata from all the mappings used to speed up partition pruning/lookup.
   * Also updates partition column statistics. Given partitionSpec must match exactly
   * one partition.
   * Returns the HdfsPartition that was dropped. If the partition does not exist, returns
   * null.
   *
   * Note: This method is not thread safe because it modifies the list of partitions
   * and the HdfsTable's partition metadata.
   */
  public HdfsPartition dropPartition(List<TPartitionKeyValue> partitionSpec) {
    HdfsPartition partition = getPartitionFromThriftPartitionSpec(partitionSpec);
    // Check if the partition does not exist.
    if (partition == null || !partitions_.remove(partition)) return null;
    totalHdfsBytes_ -= partition.getSize();
    Preconditions.checkArgument(partition.getPartitionValues().size() ==
        numClusteringCols_);
    Long partitionId = partition.getId();
    // Remove the partition id from the list of partition ids and other mappings.
    partitionIds_.remove(partitionId);
    partitionMap_.remove(partitionId);
    for (int i = 0; i < partition.getPartitionValues().size(); ++i) {
      ColumnStats stats = getColumns().get(i).getStats();
      LiteralExpr literal = partition.getPartitionValues().get(i);
      // Check if this is a null literal.
      if (literal instanceof NullLiteral) {
        nullPartitionIds_.get(i).remove(partitionId);
        stats.setNumNulls(stats.getNumNulls() - 1);
        if (nullPartitionIds_.get(i).isEmpty()) {
          stats.setNumDistinctValues(stats.getNumDistinctValues() - 1);
        }
        continue;
      }
      HashSet<Long> partitionIds = partitionValuesMap_.get(i).get(literal);
      // If there are multiple partition ids corresponding to a literal, remove
      // only this id. Otherwise, remove the <literal, id> pair.
      if (partitionIds.size() > 1) partitionIds.remove(partitionId);
      else {
        partitionValuesMap_.get(i).remove(literal);
        stats.setNumDistinctValues(stats.getNumDistinctValues() - 1);
      }
    }
    return partition;
  }

  private void addDefaultPartition(StorageDescriptor storageDescriptor)
      throws CatalogException {
    // Default partition has no files and is not referred to by scan nodes. Data sinks
    // refer to this to understand how to create new partitions.
    HdfsStorageDescriptor hdfsStorageDescriptor =
        HdfsStorageDescriptor.fromStorageDescriptor(this.name_, storageDescriptor);
    HdfsPartition partition = HdfsPartition.defaultPartition(this, hdfsStorageDescriptor);
    partitions_.add(partition);
  }

  /**
   * Populate metadata for this HdfsTable. This is similar to load() below but without
   * interacting with the HMS client. In case the table is partitioned, it doesn't load
   * partitions - partitions are handled when "ALTER TABLE ADD PARTITION ..." request
   * is processed.
   * This method is only supposed to be called for RecordService path request.
   */
  public void populateMetadata(org.apache.hadoop.hive.metastore.api.Table msTbl)
    throws TableLoadingException {
    numHdfsFiles_ = 0;
    totalHdfsBytes_ = 0;
    LOG.debug("populate metadata for: " + getFullName());

    // Set nullPartitionKeyValue from the hive conf.
    // For RecordService tmp table this is not used, since on the server side it
    // only loads partitions with non-NULL values.
    // TODO: think about how to handle default partitions on server side.
    nullPartitionKeyValue_ = "__HIVE_DEFAULT_PARTITION__";

    // Set NULL indicator string from table properties
    nullColumnValue_ =
        msTbl.getParameters().get(serdeConstants.SERIALIZATION_NULL_FORMAT);
    if (nullColumnValue_ == null) nullColumnValue_ = DEFAULT_NULL_COLUMN_VALUE;

    try {
      loadFieldSchemas(msTbl);

      // Add all columns to the table. Ordering is important: partition columns first,
      // then all other columns.
      addColumnsFromFieldSchemas(msTbl.getPartitionKeys());
      addColumnsFromFieldSchemas(nonPartFieldSchemas_);

      // If there is no partition for this table, add a default partition
      // which include all the files under the base dir.
      if (msTbl.getPartitionKeys().isEmpty()) {
        List<org.apache.hadoop.hive.metastore.api.Partition> msPartitions =
            Lists.newArrayList();
        loadPartitions(msPartitions, msTbl, null);
      }
    } catch (TableLoadingException e) {
      throw e;
    } catch (Exception e) {
      throw new TableLoadingException(
        "Failed to load metadata for table: " + getFullName(), e);
    }
  }

  @Override
  /**
   * Load the table metadata and reuse metadata to speed up metadata loading.
   * If the lastDdlTime has not been changed, that means the Hive metastore metadata has
   * not been changed. Reuses the old Hive partition metadata from cachedEntry.
   * To speed up Hdfs metadata loading, if a file's mtime has not been changed, reuses
   * the old file block metadata from old value.
   *
   * There are several cases where the cachedEntry might be reused incorrectly:
   * 1. an ALTER TABLE ADD PARTITION or dynamic partition insert is executed through
   *    Hive. This does not update the lastDdlTime.
   * 2. Hdfs rebalancer is executed. This changes the block locations but won't update
   *    the mtime (file modification time).
   * If any of these occurs, user has to execute "invalidate metadata" to invalidate the
   * metadata cache of the table to trigger a fresh load.
   */
  public void load(Table cachedEntry, HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException {
    load(cachedEntry, client, msTbl, null);
  }

  @Override
  public void load(Table cachedEntry, HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl, MetaStoreClientPool pool)
      throws TableLoadingException {
    // Start timer for loadTblMetadata.
    final Timer.Context loadTblMetadataTimer = Metrics.INSTANCE
        .getTimerCtx(loadTblMetadataTimerName_);

    numHdfsFiles_ = 0;
    totalHdfsBytes_ = 0;
    LOG.debug("load table: " + getFullName());

    // turn all exceptions into TableLoadingException
    try {
      // set nullPartitionKeyValue from the hive conf.
      nullPartitionKeyValue_ = client.getConfigValue(
          "hive.exec.default.partition.name", "__HIVE_DEFAULT_PARTITION__");

      // set NULL indicator string from table properties
      nullColumnValue_ =
          msTbl.getParameters().get(serdeConstants.SERIALIZATION_NULL_FORMAT);
      if (nullColumnValue_ == null) nullColumnValue_ = DEFAULT_NULL_COLUMN_VALUE;

      loadFieldSchemas(msTbl);

      // Add all columns to the table. Ordering is important: partition columns first,
      // then all other columns.
      addColumnsFromFieldSchemas(msTbl.getPartitionKeys());
      addColumnsFromFieldSchemas(nonPartFieldSchemas_);
      loadAllColumnStats(client);

      // Start timer for collectPartitions.
      Timer.Context listPartitionsTimer = Metrics.INSTANCE
          .getTimerCtx(listPartitionsTimerName_);

      // Firstly create a set to hold all partition names.
      Set<String> partNames = Sets.newHashSet();

      // Collect the list of partitions to use for the table. Partitions may be reused
      // from the existing cached table entry (if one exists), read from the metastore,
      // or a mix of both. Whether or not a partition is reused depends on whether
      // the table or partition has been modified.
      List<org.apache.hadoop.hive.metastore.api.Partition> msPartitions =
          Lists.newArrayList();

      // Update the partition name list. Partitions may be reused from the existing cached
      // table entry (if one exists).
      updatePartitionNameList(cachedEntry, client, partNames, msPartitions);

      // Collect the list of partitions according to partition names.
      // No need to make the metastore call if no partitions are to be updated.
      if (!partNames.isEmpty()) {
        if (enableMultithreadedMetadataLoading_) {
          msPartitions.addAll(MetaStoreUtil.fetchPartitionsByName(
              pool, Lists.newArrayList(partNames), db_.getName(), name_,
              NUM_PARTITION_FETCH_RETRIES));
        } else {
          msPartitions.addAll(MetaStoreUtil.fetchPartitionsByName(
              client, Lists.newArrayList(partNames), db_.getName(), name_,
              NUM_PARTITION_FETCH_RETRIES));
        }
      }

      Map<String, List<FileDescriptor>> oldFileDescMap = null;
      if (cachedEntry != null && cachedEntry instanceof HdfsTable) {
        HdfsTable cachedHdfsTable = (HdfsTable) cachedEntry;
        oldFileDescMap = cachedHdfsTable.fileDescMap_;
        hostIndex_.populate(cachedHdfsTable.hostIndex_.getList());
      }

      // Stop timer for collectPartitions.
      LOG.info("{}: {}millisec", listPartitionsTimerName_, listPartitionsTimer.stop()
          / Metrics.NANOTOMILLISEC);

      loadPartitions(msPartitions, msTbl, oldFileDescMap);

      // load table stats
      numRows_ = getRowCount(msTbl.getParameters());
      LOG.debug("table #rows=" + Long.toString(numRows_));

      // For unpartitioned tables set the numRows in its partitions
      // to the table's numRows.
      if (numClusteringCols_ == 0 && !partitions_.isEmpty()) {
        // Unpartitioned tables have a 'dummy' partition and a default partition.
        // Temp tables used in CTAS statements have one partition.
        Preconditions.checkState(partitions_.size() == 2 || partitions_.size() == 1);
        for (HdfsPartition p: partitions_) {
          p.setNumRows(numRows_);
        }
      }
    } catch (TableLoadingException e) {
      throw e;
    } catch (Exception e) {
      throw new TableLoadingException("Failed to load metadata for table: " +
          getFullName(), e);
    } finally {
      // Stop timer for loadTblMetadata.
      LOG.info("{}: {}millisec", loadTblMetadataTimerName_, loadTblMetadataTimer.stop()
          / Metrics.NANOTOMILLISEC);
    }
  }

  /**
   * Populates nonPartFieldSchemas_ from msTbl.
   */
  private void loadFieldSchemas(
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException {
    List<FieldSchema> msColDefs = msTbl.getSd().getCols();
    String inputFormat = msTbl.getSd().getInputFormat();
    if (HdfsFileFormat.fromJavaClassName(inputFormat) == HdfsFileFormat.AVRO) {
      // Look for the schema in TBLPROPERTIES and in SERDEPROPERTIES, with the latter
      // taking precedence.
      List<Map<String, String>> schemaSearchLocations = Lists.newArrayList();
      schemaSearchLocations.add(
          getMetaStoreTable().getSd().getSerdeInfo().getParameters());
      schemaSearchLocations.add(getMetaStoreTable().getParameters());

      avroSchema_ = HdfsTable.getAvroSchema(schemaSearchLocations);
      if (avroSchema_ == null) {
        // No Avro schema was explicitly set in the table metadata, so infer the Avro
        // schema from the column definitions.
        Schema avroSchema = AvroSchemaConverter.convertFieldSchemas(
            msColDefs, getFullName());
        avroSchema_ = avroSchema.toString();
      }
      String serdeLib = msTbl.getSd().getSerdeInfo().getSerializationLib();
      if (serdeLib == null ||
          serdeLib.equals("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")) {
        // If the SerDe library is null or set to LazySimpleSerDe, it indicates there is
        // an issue with the table metadata since Avro table need a non-native serde.
        // Instead of failing to load the table, fall back to using the fields from the
        // storage descriptor (same as Hive).
        nonPartFieldSchemas_.addAll(msColDefs);
      } else {
        // Generate new FieldSchemas from the Avro schema. This step reconciles
        // differences in the column definitions and the Avro schema. For
        // Impala-created tables this step is not necessary because the same
        // resolution is done during table creation. But Hive-created tables
        // store the original column definitions, and not the reconciled ones.
        // The schemas are reconciled according to following policy:
        //
        // Mismatched number of columns -> Prefer Avro column.
        // Mismatched name/type -> Prefer Avro column, except:
        //   A CHAR/VARCHAR column definition maps to an Avro STRING, and is preserved
        //   as a CHAR/VARCHAR in the reconciled schema.
        // Behavior for TIMESTAMP:
        // A TIMESTAMP column definition maps to an Avro STRING and is presented as a
        // STRING in the reconciled schema, because Avro has no binary TIMESTAMP
        // representation. As a result, no Avro table may have a TIMESTAMP column.
        //
        // TODO: Factor out common schema reconciliation logic from here and
        // CreateTableStmt.
        List<Column> avroCols = AvroSchemaParser.parse(avroSchema_);
        for (int i = 0; i < avroCols.size(); ++i) {
          boolean useAvroType = true;
          if (msColDefs.size() == avroCols.size()) {
            Type colDefType = parseColumnType(msColDefs.get(i));
            if (avroCols.get(i).getType().isStringType() && colDefType.isStringType()) {
              // Preserve CHAR/VARCHAR from column definition. Avro only has STRING.
              useAvroType = false;
            }
          }
          FieldSchema newFs = new FieldSchema();
          newFs.setName(avroCols.get(i).getName());
          newFs.setComment("from deserializer");
          if (useAvroType) {
            newFs.setType(avroCols.get(i).getType().toSql());
          } else {
            newFs.setType(msColDefs.get(i).getType());
          }
          nonPartFieldSchemas_.add(newFs);
        }
      }
    } else {
      nonPartFieldSchemas_.addAll(msColDefs);
    }
    // The number of clustering columns is the number of partition keys.
    numClusteringCols_ = msTbl.getPartitionKeys().size();
  }

  /**
   * Update the partition name list. It will add all partitions into the 'partNames' if
   * there is no cache, and remove the one which is in cache and not modified, and add it
   * to partition list.
   */
  private void updatePartitionNameList(Table cachedEntry, HiveMetaStoreClient client,
      Set<String> partNames,
      List<org.apache.hadoop.hive.metastore.api.Partition> msPartitions)
      throws MetaException, TException {
    if (cachedEntry == null || !(cachedEntry instanceof HdfsTable) ||
        cachedEntry.lastDdlTime_ != lastDdlTime_) {
      partNames.addAll(client.listPartitionNames(db_.getName(), name_, (short) -1));
    } else {
      // The table was already in the metadata cache and it has not been modified.
      Preconditions.checkArgument(cachedEntry instanceof HdfsTable);
      HdfsTable cachedHdfsTableEntry = (HdfsTable) cachedEntry;

      // If these are not the exact same object, look up the set of partition names in
      // the metastore. This is to support the special case of CTAS which creates a
      // "temp" table that doesn't actually exist in the metastore.
      if (cachedEntry != this) {
        // Since the table has not been modified, we might be able to reuse some of the
        // old partition metadata if the individual partitions have not been modified.
        // First get a list of all the partition names for this table from the
        // metastore, this is much faster than listing all the Partition objects.
        partNames.addAll(client.listPartitionNames(db_.getName(), name_, (short) -1));
      }

      int totalPartitions = partNames.size();
      // Get all the partitions from the cached entry that have not been modified.
      for (HdfsPartition cachedPart: cachedHdfsTableEntry.getPartitions()) {
        // Skip the default partition and any partitions that have been modified.
        if (cachedPart.isDirty() || cachedPart.isDefaultPartition()) {
          continue;
        }

        org.apache.hadoop.hive.metastore.api.Partition cachedMsPart =
            cachedPart.toHmsPartition();
        if (cachedMsPart == null) continue;

        // This is a partition we already know about and it hasn't been modified.
        // No need to reload the metadata.
        String cachedPartName = cachedPart.getPartitionName();
        if (partNames.contains(cachedPartName)) {
          msPartitions.add(cachedMsPart);
          partNames.remove(cachedPartName);;
        }
      }
      LOG.info(String.format("Incrementally refreshing %d/%d partitions.",
          partNames.size(), totalPartitions));
    }
  }

  /**
   * Gets an Avro table's JSON schema from the list of given table property search
   * locations. The schema may be specified as a string literal or provided as a
   * Hadoop FileSystem or http URL that points to the schema. Apart from ensuring
   * that the JSON schema is not SCHEMA_NONE, this function does not perform any
   * additional validation on the returned string (e.g., it may not be a valid
   * schema). Returns the Avro schema or null if none was specified in the search
   * locations. Throws a TableLoadingException if a schema was specified, but could not
   * be retrieved, e.g., because of an invalid URL.
   */
  public static String getAvroSchema(List<Map<String, String>> schemaSearchLocations)
      throws TableLoadingException {
    String url = null;
    // Search all locations and break out on the first valid schema found.
    for (Map<String, String> schemaLocation: schemaSearchLocations) {
      if (schemaLocation == null) continue;

      String literal = schemaLocation.get(AvroSerdeUtils.SCHEMA_LITERAL);
      if (literal != null && !literal.equals(AvroSerdeUtils.SCHEMA_NONE)) return literal;

      url = schemaLocation.get(AvroSerdeUtils.SCHEMA_URL);
      if (url != null && !url.equals(AvroSerdeUtils.SCHEMA_NONE)) {
        url = url.trim();
        break;
      }
    }
    if (url == null) return null;

    String schema = null;
    if (url.toLowerCase().startsWith("http://")) {
      InputStream urlStream = null;
      try {
        urlStream = new URL(url).openStream();
        schema = IOUtils.toString(urlStream);
      } catch (IOException e) {
        throw new TableLoadingException("Problem reading Avro schema from: " + url, e);
      } finally {
        IOUtils.closeQuietly(urlStream);
      }
    } else {
      Path path = new Path(url);
      FileSystem fs = null;
      try {
        fs = path.getFileSystem(FileSystemUtil.getConfiguration());
      } catch (Exception e) {
        throw new TableLoadingException(String.format(
            "Invalid avro.schema.url: %s. %s", path, e.getMessage()));
      }
      StringBuilder errorMsg = new StringBuilder();
      if (!FileSystemUtil.isPathReachable(path, fs, errorMsg)) {
        throw new TableLoadingException(String.format(
            "Invalid avro.schema.url: %s. %s", path, errorMsg));
      }
      try {
        schema = FileSystemUtil.readFile(path);
      } catch (IOException e) {
        throw new TableLoadingException(
            "Problem reading Avro schema at: " + url, e);
      }
    }
    return schema;
  }

  @Override
  protected List<String> getColumnNamesWithHmsStats() {
    List<String> ret = Lists.newArrayList();
    // Only non-partition columns have column stats in the HMS.
    for (Column column: getColumns().subList(numClusteringCols_, getColumns().size())) {
      ret.add(column.getName().toLowerCase());
    }
    return ret;
  }

  @Override
  protected void loadFromThrift(TTable thriftTable) throws TableLoadingException {
    super.loadFromThrift(thriftTable);
    THdfsTable hdfsTable = thriftTable.getHdfs_table();
    hdfsBaseDir_ = hdfsTable.getHdfsBaseDir();
    nullColumnValue_ = hdfsTable.nullColumnValue;
    nullPartitionKeyValue_ = hdfsTable.nullPartitionKeyValue;
    multipleFileSystems_ = hdfsTable.multiple_filesystems;
    hostIndex_.populate(hdfsTable.getNetwork_addresses());
    resetPartitionMd();

    numHdfsFiles_ = 0;
    totalHdfsBytes_ = 0;
    for (Map.Entry<Long, THdfsPartition> part: hdfsTable.getPartitions().entrySet()) {
      HdfsPartition hdfsPart =
          HdfsPartition.fromThrift(this, part.getKey(), part.getValue());
      numHdfsFiles_ += hdfsPart.getFileDescriptors().size();
      totalHdfsBytes_ += hdfsPart.getSize();
      partitions_.add(hdfsPart);
    }
    avroSchema_ = hdfsTable.isSetAvroSchema() ? hdfsTable.getAvroSchema() : null;
    isMarkedCached_ = HdfsCachingUtil.getCacheDirectiveId(
        getMetaStoreTable().getParameters()) != null;
    populatePartitionMd();
  }

  @Override
  public TTableDescriptor toThriftDescriptor(Set<Long> referencedPartitions) {
    TTableDescriptor tableDesc = new TTableDescriptor(id_.asInt(), TTableType.HDFS_TABLE,
        getTColumnDescriptors(), numClusteringCols_, name_, db_.getName());
    tableDesc.setHdfsTable(getTHdfsTable(false, referencedPartitions));
    return tableDesc;
  }

  @Override
  public TTable toThrift() {
    // Send all metadata between the catalog service and the FE.
    TTable table = super.toThrift();
    table.setTable_type(TTableType.HDFS_TABLE);
    table.setHdfs_table(getTHdfsTable(true, null));
    return table;
  }

  /**
   * Create a THdfsTable corresponding to this HdfsTable. If includeFileDesc is true,
   * then then all partitions and THdfsFileDescs of each partition should be included.
   * Otherwise, don't include any THdfsFileDescs, and include only those partitions in
   * the refPartitions set (the backend doesn't need metadata for unreferenced
   * partitions).
   */
  private THdfsTable getTHdfsTable(boolean includeFileDesc, Set<Long> refPartitions) {
    // includeFileDesc implies all partitions should be included (refPartitions == null).
    Preconditions.checkState(!includeFileDesc || refPartitions == null);
    Map<Long, THdfsPartition> idToPartition = Maps.newHashMap();
    for (HdfsPartition partition: partitions_) {
      long id = partition.getId();
      if (refPartitions == null || refPartitions.contains(id)) {
        idToPartition.put(id, partition.toThrift(includeFileDesc));
      }
    }
    THdfsTable hdfsTable = new THdfsTable(hdfsBaseDir_, getColumnNames(),
        nullPartitionKeyValue_, nullColumnValue_, idToPartition);
    hdfsTable.setAvroSchema(avroSchema_);
    hdfsTable.setMultiple_filesystems(multipleFileSystems_);
    if (includeFileDesc) {
      // Network addresses are used only by THdfsFileBlocks which are inside
      // THdfsFileDesc, so include network addreses only when including THdfsFileDesc.
      hdfsTable.setNetwork_addresses(hostIndex_.getList());
    }
    return hdfsTable;
  }

  public long getNumHdfsFiles() { return numHdfsFiles_; }
  // Possibly be called by multiple threads when loading block locations.
  private synchronized void addNumHdfsFiles (long numFile) { numHdfsFiles_ += numFile; }
  public long getTotalHdfsBytes() { return totalHdfsBytes_; }
  public String getHdfsBaseDir() { return hdfsBaseDir_; }
  public boolean isAvroTable() { return avroSchema_ != null; }

  /**
   * Get the index of hosts that store replicas of blocks of this table.
   */
  public ListMap<TNetworkAddress> getHostIndex() { return hostIndex_; }

  /**
   * Returns the file format that the majority of partitions are stored in.
   */
  public HdfsFileFormat getMajorityFormat() {
    Map<HdfsFileFormat, Integer> numPartitionsByFormat = Maps.newHashMap();
    for (HdfsPartition partition: partitions_) {
      HdfsFileFormat format = partition.getInputFormatDescriptor().getFileFormat();
      Integer numPartitions = numPartitionsByFormat.get(format);
      if (numPartitions == null) {
        numPartitions = Integer.valueOf(1);
      } else {
        numPartitions = Integer.valueOf(numPartitions.intValue() + 1);
      }
      numPartitionsByFormat.put(format, numPartitions);
    }

    int maxNumPartitions = Integer.MIN_VALUE;
    HdfsFileFormat majorityFormat = null;
    for (Map.Entry<HdfsFileFormat, Integer> entry: numPartitionsByFormat.entrySet()) {
      if (entry.getValue().intValue() > maxNumPartitions) {
        majorityFormat = entry.getKey();
        maxNumPartitions = entry.getValue().intValue();
      }
    }
    Preconditions.checkNotNull(majorityFormat);
    return majorityFormat;
  }

  /**
   * Returns the HDFS paths corresponding to HdfsTable partitions that don't exist in
   * the metastore. An HDFS path is represented as a list of strings values, one per
   * partition key column.
   */
  public List<List<String>> getPathsWithoutPartitions() throws CatalogException {
    List<List<LiteralExpr>> existingPartitions = new ArrayList<List<LiteralExpr>>();
    // Get the list of partition values of existing partitions in metastore.
    for (HdfsPartition partition: partitions_) {
      if (partition.isDefaultPartition()) continue;
      existingPartitions.add(partition.getPartitionValues());
    }

    List<String> partitionKeys = Lists.newArrayList();
    for (int i = 0; i < numClusteringCols_; ++i) {
      partitionKeys.add(getColumns().get(i).getName());
    }
    Path basePath = new Path(hdfsBaseDir_);
    List<List<String>> partitionsNotInHms = new ArrayList<List<String>>();
    try {
      getAllPartitionsNotInHms(basePath, partitionKeys, existingPartitions,
          partitionsNotInHms);
    } catch (Exception e) {
      throw new CatalogException(String.format("Failed to recover partitions for %s " +
          "with exception:%s.", getFullName(), e));
    }
    return partitionsNotInHms;
  }

  /**
   * Returns all partitions which match the partition keys directory structure and pass
   * type compatibility check. Also these partitions are not already part of the table.
   */
  private void getAllPartitionsNotInHms(Path path, List<String> partitionKeys,
      List<List<LiteralExpr>> existingPartitions,
      List<List<String>> partitionsNotInHms) throws IOException {
    FileSystem fs = path.getFileSystem(LoadMetadataUtil.getConf());
    // Check whether the base directory exists.
    if (!fs.exists(path)) return;

    List<String> partitionValues = Lists.newArrayList();
    List<LiteralExpr> partitionExprs = Lists.newArrayList();
    getAllPartitionsNotInHms(path, partitionKeys, 0, fs, partitionValues,
        partitionExprs, existingPartitions, partitionsNotInHms);
  }

  /**
   * Returns all partitions which match the partition keys directory structure and pass
   * the type compatibility check.
   *
   * path e.g. c1=1/c2=2/c3=3
   * partitionKeys The ordered partition keys. e.g.("c1", "c2", "c3")
   * depth The start position in partitionKeys to match the path name.
   * partitionValues The partition values used to create a partition.
   * partitionExprs The list of LiteralExprs which is used to avoid duplicate partitions.
   * E.g. Having /c1=0001 and /c1=01, we should make sure only one partition
   * will be added.
   * existingPartitions All partitions which exist in metastore or newly added.
   * partitionsNotInHms Contains all the recovered partitions.
   */
  private void getAllPartitionsNotInHms(Path path, List<String> partitionKeys,
      int depth, FileSystem fs, List<String> partitionValues,
      List<LiteralExpr> partitionExprs, List<List<LiteralExpr>> existingPartitions,
      List<List<String>> partitionsNotInHms) throws IOException {
    if (depth == partitionKeys.size()) {
      if (existingPartitions.contains(partitionExprs)) {
        LOG.trace(String.format("Skip recovery of path '%s' because it already exists " +
            "in metastore", path.toString()));
      } else {
        partitionsNotInHms.add(partitionValues);
        existingPartitions.add(partitionExprs);
      }
      return;
    }

    FileStatus[] statuses = fs.listStatus(path);
    for (FileStatus status: statuses) {
      if (!status.isDirectory()) continue;
      Pair<String, LiteralExpr> keyValues =
          getTypeCompatibleValue(status.getPath(), partitionKeys.get(depth));
      if (keyValues == null) continue;

      List<String> currentPartitionValues = Lists.newArrayList(partitionValues);
      List<LiteralExpr> currentPartitionExprs = Lists.newArrayList(partitionExprs);
      currentPartitionValues.add(keyValues.first);
      currentPartitionExprs.add(keyValues.second);
      getAllPartitionsNotInHms(status.getPath(), partitionKeys, depth + 1, fs,
          currentPartitionValues, currentPartitionExprs,
          existingPartitions, partitionsNotInHms);
    }
  }

  /**
   * Checks that the last component of 'path' is of the form "<partitionkey>=<v>"
   * where 'v' is a type-compatible value from the domain of the 'partitionKey' column.
   * If not, returns null, otherwise returns a Pair instance, the first element is the
   * original value, the second element is the LiteralExpr created from the original
   * value.
   */
  private Pair<String, LiteralExpr> getTypeCompatibleValue(Path path, String partitionKey) {
    String partName[] = path.getName().split("=");
    if (partName.length != 2 || !partName[0].equals(partitionKey)) return null;

    // Check Type compatibility for Partition value.
    Column column = getColumn(partName[0]);
    Preconditions.checkNotNull(column);
    Type type = column.getType();
    LiteralExpr expr = null;
    if (!partName[1].equals(getNullPartitionKeyValue())) {
      try {
        expr = LiteralExpr.create(partName[1], type);
        // Skip large value which exceeds the MAX VALUE of specified Type.
        if (expr instanceof NumericLiteral) {
          if (NumericLiteral.isOverflow(((NumericLiteral)expr).getValue(), type)) {
            LOG.warn(String.format("Skip the overflow value (%s) for Type (%s).",
                partName[1], type.toSql()));
            return null;
          }
        }
      } catch (Exception ex) {
        LOG.debug(String.format("Invalid partition value (%s) for Type (%s).",
            partName[1], type.toSql()));
        return null;
      }
    } else {
      expr = new NullLiteral();
    }
    return new Pair<String, LiteralExpr>(partName[1], expr);
  }

  /**
   * Returns statistics on this table as a tabular result set. Used for the
   * SHOW TABLE STATS statement. The schema of the returned TResultSet is set
   * inside this method.
   */
  public TResultSet getTableStats() {
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    result.setSchema(resultSchema);

    for (int i = 0; i < numClusteringCols_; ++i) {
      // Add the partition-key values as strings for simplicity.
      Column partCol = getColumns().get(i);
      TColumn colDesc = new TColumn(partCol.getName(), Type.STRING.toThrift());
      resultSchema.addToColumns(colDesc);
    }

    resultSchema.addToColumns(new TColumn("#Rows", Type.BIGINT.toThrift()));
    resultSchema.addToColumns(new TColumn("#Files", Type.BIGINT.toThrift()));
    resultSchema.addToColumns(new TColumn("Size", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Bytes Cached", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Cache Replication", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Format", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Incremental stats", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Location", Type.STRING.toThrift()));

    // Pretty print partitions and their stats.
    ArrayList<HdfsPartition> orderedPartitions = Lists.newArrayList(partitions_);
    Collections.sort(orderedPartitions);

    long totalCachedBytes = 0L;
    for (HdfsPartition p: orderedPartitions) {
      // Ignore dummy default partition.
      if (p.getId() == ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID) continue;
      TResultRowBuilder rowBuilder = new TResultRowBuilder();

      // Add the partition-key values (as strings for simplicity).
      for (LiteralExpr expr: p.getPartitionValues()) {
        rowBuilder.add(expr.getStringValue());
      }

      // Add number of rows, files, bytes, cache stats, and file format.
      rowBuilder.add(p.getNumRows()).add(p.getFileDescriptors().size())
          .addBytes(p.getSize());
      if (!p.isMarkedCached()) {
        // Helps to differentiate partitions that have 0B cached versus partitions
        // that are not marked as cached.
        rowBuilder.add("NOT CACHED");
        rowBuilder.add("NOT CACHED");
      } else {
        // Calculate the number the number of bytes that are cached.
        long cachedBytes = 0L;
        for (FileDescriptor fd: p.getFileDescriptors()) {
          for (THdfsFileBlock fb: fd.getFileBlocks()) {
            if (fb.getIs_replica_cached().contains(true)) {
              cachedBytes += fb.getLength();
            }
          }
        }
        totalCachedBytes += cachedBytes;
        rowBuilder.addBytes(cachedBytes);

        // Extract cache replication factor from the parameters of the table
        // if the table is not partitioned or directly from the partition.
        Short rep = HdfsCachingUtil.getCachedCacheReplication(
            numClusteringCols_ == 0 ?
            p.getTable().getMetaStoreTable().getParameters() :
            p.getParameters());
        rowBuilder.add(rep.toString());
      }
      rowBuilder.add(p.getInputFormatDescriptor().getFileFormat().toString());

      rowBuilder.add(String.valueOf(p.hasIncrementalStats()));
      rowBuilder.add(p.getLocation());
      result.addToRows(rowBuilder.get());
    }

    // For partitioned tables add a summary row at the bottom.
    if (numClusteringCols_ > 0) {
      TResultRowBuilder rowBuilder = new TResultRowBuilder();
      int numEmptyCells = numClusteringCols_ - 1;
      rowBuilder.add("Total");
      for (int i = 0; i < numEmptyCells; ++i) {
        rowBuilder.add("");
      }

      // Total num rows, files, and bytes (leave format empty).
      rowBuilder.add(numRows_).add(numHdfsFiles_).addBytes(totalHdfsBytes_)
          .addBytes(totalCachedBytes).add("").add("").add("").add("");
      result.addToRows(rowBuilder.get());
    }
    return result;
  }

  /**
   * Returns files info for the given dbname/tableName and partition spec.
   * Returns files info for all partitions if partition spec is null.
   */
  public TResultSet getFiles(List<TPartitionKeyValue> partitionSpec)
      throws CatalogException {
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    result.setSchema(resultSchema);
    resultSchema.addToColumns(new TColumn("path", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("size", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("partition", Type.STRING.toThrift()));
    result.setRows(Lists.<TResultRow>newArrayList());

    List<HdfsPartition> partitions = null;
    if (partitionSpec == null) {
      partitions = partitions_;
    } else {
      // Get the HdfsPartition object for the given partition spec.
      HdfsPartition partition = getPartitionFromThriftPartitionSpec(partitionSpec);
      Preconditions.checkState(partition != null);
      partitions = Lists.newArrayList(partition);
    }

    for (HdfsPartition p: partitions) {
      for (FileDescriptor fd: p.getFileDescriptors()) {
        TResultRowBuilder rowBuilder = new TResultRowBuilder();
        rowBuilder.add(p.getLocation() + "/" + fd.getFileName());
        rowBuilder.add(PrintUtils.printBytes(fd.getFileLength()));
        rowBuilder.add(p.getPartitionName());
        result.addToRows(rowBuilder.get());
      }
    }
    return result;
  }
}
