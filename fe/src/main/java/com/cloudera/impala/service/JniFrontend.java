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

package com.cloudera.impala.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.ToSqlUtils;
import com.cloudera.impala.authorization.AuthorizationConfig;
import com.cloudera.impala.authorization.ImpalaInternalAdminUser;
import com.cloudera.impala.authorization.SentryConfig;
import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.CatalogException;
import com.cloudera.impala.catalog.DataSource;
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.ImpaladCatalog;
import com.cloudera.impala.catalog.RecordServiceCatalog;
import com.cloudera.impala.catalog.Role;
import com.cloudera.impala.common.FileSystemUtil;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.JniUtil;
import com.cloudera.impala.security.AuthenticationException;
import com.cloudera.impala.security.DelegationTokenManager;
import com.cloudera.impala.thrift.TAuthorizePathRequest;
import com.cloudera.impala.thrift.TAuthorizePathResponse;
import com.cloudera.impala.thrift.TCancelDelegationTokenRequest;
import com.cloudera.impala.thrift.TCatalogObject;
import com.cloudera.impala.thrift.TDescribeTableParams;
import com.cloudera.impala.thrift.TDescribeTableResult;
import com.cloudera.impala.thrift.TExecRequest;
import com.cloudera.impala.thrift.TFunctionCategory;
import com.cloudera.impala.thrift.TGetAllHadoopConfigsResponse;
import com.cloudera.impala.thrift.TGetDataSrcsParams;
import com.cloudera.impala.thrift.TGetDataSrcsResult;
import com.cloudera.impala.thrift.TGetDbsParams;
import com.cloudera.impala.thrift.TGetDbsResult;
import com.cloudera.impala.thrift.TGetDelegationTokenRequest;
import com.cloudera.impala.thrift.TGetDelegationTokenResponse;
import com.cloudera.impala.thrift.TGetFunctionsParams;
import com.cloudera.impala.thrift.TGetFunctionsResult;
import com.cloudera.impala.thrift.TGetHadoopConfigRequest;
import com.cloudera.impala.thrift.TGetHadoopConfigResponse;
import com.cloudera.impala.thrift.TGetMasterKeyRequest;
import com.cloudera.impala.thrift.TGetMasterKeyResponse;
import com.cloudera.impala.thrift.TGetTablesParams;
import com.cloudera.impala.thrift.TGetTablesResult;
import com.cloudera.impala.thrift.TLoadDataReq;
import com.cloudera.impala.thrift.TLoadDataResp;
import com.cloudera.impala.thrift.TLogLevel;
import com.cloudera.impala.thrift.TMetadataOpRequest;
import com.cloudera.impala.thrift.TQueryCtx;
import com.cloudera.impala.thrift.TRenewDelegationTokenRequest;
import com.cloudera.impala.thrift.TResultSet;
import com.cloudera.impala.thrift.TRetrieveUserAndPasswordRequest;
import com.cloudera.impala.thrift.TRetrieveUserAndPasswordResponse;
import com.cloudera.impala.thrift.TShowFilesParams;
import com.cloudera.impala.thrift.TShowGrantRoleParams;
import com.cloudera.impala.thrift.TShowRolesParams;
import com.cloudera.impala.thrift.TShowRolesResult;
import com.cloudera.impala.thrift.TShowStatsParams;
import com.cloudera.impala.thrift.TTableName;
import com.cloudera.impala.thrift.TUpdateCatalogCacheRequest;
import com.cloudera.impala.thrift.TUpdateMembershipRequest;
import com.cloudera.impala.util.GlogAppender;
import com.cloudera.impala.util.TSessionStateUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * JNI-callable interface onto a wrapped Frontend instance. The main point is to serialise
 * and deserialise thrift structures between C and Java.
 */
public class JniFrontend {
  private final static Logger LOG = LoggerFactory.getLogger(JniFrontend.class);
  private final static TBinaryProtocol.Factory protocolFactory_ =
      new TBinaryProtocol.Factory();
  private final static TCompactProtocol.Factory compactProtocolFactory_ =
      new TCompactProtocol.Factory();
  private final Frontend frontend_;

  // Required minimum value (in milliseconds) for the HDFS config
  // 'dfs.client.file-block-storage-locations.timeout.millis'
  private static final long MIN_DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS =
      10 * 1000;

  // True if running the RecordService planner/worker services.
  private final boolean runningPlanner_;
  private final boolean runningWorker_;

  /**
   * Create a new instance of the Jni Frontend.
   */
  public JniFrontend(boolean loadInBackground, String serverName,
      String authorizationPolicyFile, String sentryConfigFile,
      String authPolicyProviderClass, int impalaLogLevel, int otherLogLevel,
      boolean runningPlanner, boolean runningWorker,
      int numMetadataLoadingThreads, String rsTmpDb) throws InternalException {
    runningPlanner_ = runningPlanner;
    runningWorker_ = runningWorker;

    Preconditions.checkState(rsTmpDb != null && !rsTmpDb.isEmpty(),
        "RecordService tmp database name is not allowed to be null or empty");

    GlogAppender.Install(TLogLevel.values()[impalaLogLevel],
        TLogLevel.values()[otherLogLevel]);

    // Validate the authorization configuration before initializing the Frontend.
    // If there are any configuration problems Impala startup will fail.
    AuthorizationConfig authConfig;
    if (runningPlanner || runningWorker) {
      // For RecordService, serverName, authorizationPolicyFile, etc., are directly
      // loaded from Sentry config file.
      authConfig = new AuthorizationConfig(sentryConfigFile);
    } else {
      authConfig = new AuthorizationConfig(serverName,
        authorizationPolicyFile, sentryConfigFile, authPolicyProviderClass);
    }
    authConfig.validateConfig();
    if (authConfig.isEnabled()) {
      LOG.info(String.format("Authorization is 'ENABLED' with server name: %s, config "
          + "file: %s, using %s", authConfig.getServerName(),
          authConfig.getSentryConfig().getConfigFile(),
          authConfig.isFileBasedPolicy() ? "file based policy from: " +
          authConfig.getPolicyFile() : "Sentry Policy Service."));
    } else {
      LOG.info("Authorization is 'DISABLED'.");
    }
    LOG.info(JniUtil.getJavaVersion());

    if (isRecordService()) {
      if (runningPlanner_) {
        // Check if the Sentry Service is configured. If so, create a configuration
        // object.
        SentryConfig sentryConfig = null;
        if (!Strings.isNullOrEmpty(sentryConfigFile)) {
          sentryConfig = new SentryConfig(sentryConfigFile);
          sentryConfig.loadConfig();
        }
        // recordserviced directly uses RecordServiceCatalog, meaning it does not
        // use the statestored to load metadata but goes directly to the other
        // services (HDFS, HMS, etc).
        RecordServiceCatalog catalog = new RecordServiceCatalog(
            numMetadataLoadingThreads, rsTmpDb, sentryConfig, JniCatalog.generateId());
        frontend_ = new Frontend(authConfig, catalog);
      } else {
        // Just running worker, no need to start catalog.
        frontend_ = new Frontend(authConfig, null);
      }
    } else {
      frontend_ = new Frontend(authConfig, new ImpaladCatalog());
    }
  }

  /**
   * Initializes zookeeper for delegation tokens and/or membership.
   */
  public void initZooKeeper(String serviceName, String principal, String keytabPath,
      int plannerPort, int workerPort) throws InternalException {
    Preconditions.checkArgument(plannerPort > 0 || workerPort > 0,
        "ZooKeeper cannot be initialized when neither" +
            " planner nor worker is running. ");
    ZooKeeperSession zkSession = null;
    // Start up zookeeper/curator connection.
    try {
      zkSession = new ZooKeeperSession(CONF, serviceName, principal, keytabPath,
          plannerPort, workerPort);
    } catch (IOException e) {
      throw new InternalException("Could not start up zookeeper session.", e);
    }

    if (principal != null && !principal.isEmpty()) {
      try {
        DelegationTokenManager.init(CONF, runningPlanner_, zkSession);
      } catch (IOException e) {
        throw new InternalException("Could not initialize delegation token manager", e);
      }
    }
  }

  /**
   * Jni wrapper for Frontend.createExecRequest(). Accepts a serialized
   * TQueryContext; returns a serialized TQueryExecRequest.
   */
  public byte[] createExecRequest(byte[] thriftQueryContext)
      throws ImpalaException {
    TQueryCtx queryCtx = new TQueryCtx();
    JniUtil.deserializeThrift(protocolFactory_, queryCtx, thriftQueryContext);

    StringBuilder explainString = new StringBuilder();
    TExecRequest result = frontend_.createExecRequest(queryCtx, explainString);
    if (explainString.length() > 0) LOG.debug(explainString.toString());

    // TODO: avoid creating serializer for each query?
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Jni wrapper for Frontend.createRecordServiceExecRequest(). Accepts a serialized
   * TQueryCtx; returns a serialized TRecordServiceExecRequest.
   */
  public byte[] createRecordServiceExecRequest(byte[] thriftQueryContext)
      throws ImpalaException {
    TQueryCtx queryCtx = new TQueryCtx();
    JniUtil.deserializeThrift(protocolFactory_, queryCtx, thriftQueryContext);

    StringBuilder explainString = new StringBuilder();
    TExecRequest result =
        frontend_.createRecordServiceExecRequest(queryCtx, explainString);
    if (explainString.length() > 0) LOG.debug(explainString.toString());

    // TODO: avoid creating serializer for each query?
    TSerializer serializer = new TSerializer(compactProtocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  public byte[] updateCatalogCache(byte[] thriftCatalogUpdate) throws ImpalaException {
    TUpdateCatalogCacheRequest req = new TUpdateCatalogCacheRequest();
    JniUtil.deserializeThrift(protocolFactory_, req, thriftCatalogUpdate);
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(frontend_.updateCatalogCache(req));
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Jni wrapper for Frontend.updateMembership(). Accepts a serialized
   * TUpdateMembershipRequest.
   */
  public void updateMembership(byte[] thriftMembershipUpdate) throws ImpalaException {
    TUpdateMembershipRequest req = new TUpdateMembershipRequest();
    JniUtil.deserializeThrift(protocolFactory_, req, thriftMembershipUpdate);
    frontend_.updateMembership(req);
  }

  public byte[] authorizePath(byte[] thriftAuthorizePathParams) throws ImpalaException {
    TAuthorizePathRequest req = new TAuthorizePathRequest();
    JniUtil.deserializeThrift(protocolFactory_, req, thriftAuthorizePathParams);
    TAuthorizePathResponse response = new TAuthorizePathResponse();
    response.success = frontend_.authorizePath(req.username, req.path);
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(response);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Loads a table or partition with one or more data files. If the "overwrite" flag
   * in the request is true, all existing data in the table/partition will be replaced.
   * If the "overwrite" flag is false, the files will be added alongside any existing
   * data files.
   */
  public byte[] loadTableData(byte[] thriftLoadTableDataParams)
      throws ImpalaException, IOException {
    TLoadDataReq request = new TLoadDataReq();
    JniUtil.deserializeThrift(protocolFactory_, request, thriftLoadTableDataParams);
    TLoadDataResp response = frontend_.loadTableData(request);
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(response);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Return an explain plan based on thriftQueryContext, a serialized TQueryContext.
   * This call is thread-safe.
   */
  public String getExplainPlan(byte[] thriftQueryContext) throws ImpalaException {
    TQueryCtx queryCtx = new TQueryCtx();
    JniUtil.deserializeThrift(protocolFactory_, queryCtx, thriftQueryContext);
    String plan = frontend_.getExplainString(queryCtx);
    LOG.debug("Explain plan: " + plan);
    return plan;
  }

  /**
   * Returns a list of table names matching an optional pattern.
   * The argument is a serialized TGetTablesParams object.
   * The return type is a serialised TGetTablesResult object.
   * @see Frontend#getTableNames
   */
  public byte[] getTableNames(byte[] thriftGetTablesParams) throws ImpalaException {
    TGetTablesParams params = new TGetTablesParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftGetTablesParams);
    // If the session was not set it indicates this is an internal Impala call.
    User user = params.isSetSession() ?
        new User(TSessionStateUtil.getEffectiveUser(params.getSession())) :
        ImpalaInternalAdminUser.getInstance();

    Preconditions.checkState(!params.isSetSession() || user != null );
    List<String> tables = frontend_.getTableNames(params.db, params.pattern, user);

    TGetTablesResult result = new TGetTablesResult();
    result.setTables(tables);

    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns files info of a table or partition.
   * The argument is a serialized TShowFilesParams object.
   * The return type is a serialised TResultSet object.
   * @see Frontend#getTableFiles
   */
  public byte[] getTableFiles(byte[] thriftShowFilesParams) throws ImpalaException {
    TShowFilesParams params = new TShowFilesParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftShowFilesParams);
    TResultSet result = frontend_.getTableFiles(params);

    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns a list of table names matching an optional pattern.
   * The argument is a serialized TGetTablesParams object.
   * The return type is a serialised TGetTablesResult object.
   * @see Frontend#getTableNames
   */
  public byte[] getDbNames(byte[] thriftGetTablesParams) throws ImpalaException {
    TGetDbsParams params = new TGetDbsParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftGetTablesParams);
    // If the session was not set it indicates this is an internal Impala call.
    User user = params.isSetSession() ?
        new User(TSessionStateUtil.getEffectiveUser(params.getSession())) :
        ImpalaInternalAdminUser.getInstance();
    List<String> dbs = frontend_.getDbNames(params.pattern, user);

    TGetDbsResult result = new TGetDbsResult();
    result.setDbs(dbs);

    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns a list of data sources matching an optional pattern.
   * The argument is a serialized TGetDataSrcsResult object.
   * The return type is a serialised TGetDataSrcsResult object.
   * @see Frontend#getDataSrcs
   */
  public byte[] getDataSrcMetadata(byte[] thriftParams) throws ImpalaException {
    TGetDataSrcsParams params = new TGetDataSrcsParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftParams);

    TGetDataSrcsResult result = new TGetDataSrcsResult();
    List<DataSource> dataSources = frontend_.getDataSrcs(params.pattern);
    result.setData_src_names(Lists.<String>newArrayListWithCapacity(dataSources.size()));
    result.setLocations(Lists.<String>newArrayListWithCapacity(dataSources.size()));
    result.setClass_names(Lists.<String>newArrayListWithCapacity(dataSources.size()));
    result.setApi_versions(Lists.<String>newArrayListWithCapacity(dataSources.size()));
    for (DataSource dataSource: dataSources) {
      result.addToData_src_names(dataSource.getName());
      result.addToLocations(dataSource.getLocation());
      result.addToClass_names(dataSource.getClassName());
      result.addToApi_versions(dataSource.getApiVersion());
    }
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  public byte[] getStats(byte[] thriftShowStatsParams) throws ImpalaException {
    TShowStatsParams params = new TShowStatsParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftShowStatsParams);
    Preconditions.checkState(params.isSetTable_name());
    TResultSet result;
    if (params.isIs_show_col_stats()) {
      result = frontend_.getColumnStats(params.getTable_name().getDb_name(),
          params.getTable_name().getTable_name());
    } else {
      result = frontend_.getTableStats(params.getTable_name().getDb_name(),
          params.getTable_name().getTable_name());
    }
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns a list of function names matching an optional pattern.
   * The argument is a serialized TGetFunctionsParams object.
   * The return type is a serialised TGetFunctionsResult object.
   * @see Frontend#getTableNames
   */
  public byte[] getFunctions(byte[] thriftGetFunctionsParams) throws ImpalaException {
    TGetFunctionsParams params = new TGetFunctionsParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftGetFunctionsParams);

    TGetFunctionsResult result = new TGetFunctionsResult();
    List<String> signatures = Lists.newArrayList();
    List<String> retTypes = Lists.newArrayList();
    List<Function> fns = frontend_.getFunctions(params.category, params.db,
        params.pattern, false);
    for (Function fn: fns) {
      signatures.add(fn.signatureString());
      retTypes.add(fn.getReturnType().toString());
    }
    result.setFn_signatures(signatures);
    result.setFn_ret_types(retTypes);
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Gets the thrift representation of a catalog object.
   */
  public byte[] getCatalogObject(byte[] thriftParams) throws ImpalaException,
      TException {
    TCatalogObject objectDescription = new TCatalogObject();
    JniUtil.deserializeThrift(protocolFactory_, objectDescription, thriftParams);
    TSerializer serializer = new TSerializer(protocolFactory_);
    return serializer.serialize(
        frontend_.getCatalog().getTCatalogObject(objectDescription));
  }

  /**
   * Returns a list of the columns making up a table.
   * The argument is a serialized TDescribeTableParams object.
   * The return type is a serialised TDescribeTableResult object.
   * @see Frontend#describeTable
   */
  public byte[] describeTable(byte[] thriftDescribeTableParams) throws ImpalaException {
    TDescribeTableParams params = new TDescribeTableParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftDescribeTableParams);

    TDescribeTableResult result = frontend_.describeTable(
        params.getDb(), params.getTable_name(), params.getOutput_style(),
        params.getResult_struct());

    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns a SQL DDL string for creating the specified table.
   */
  public String showCreateTable(byte[] thriftTableName)
      throws ImpalaException {
    TTableName params = new TTableName();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftTableName);
    return ToSqlUtils.getCreateTableSql(frontend_.getCatalog().getTable(
        params.getDb_name(), params.getTable_name()));
  }

  /**
   * Returns a SQL DDL string for creating the specified function.
   */
  public String showCreateFunction(byte[] thriftShowCreateFunctionParams)
      throws ImpalaException {
    TGetFunctionsParams params = new TGetFunctionsParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftShowCreateFunctionParams);
    Preconditions.checkArgument(params.category == TFunctionCategory.SCALAR ||
        params.category == TFunctionCategory.AGGREGATE);
    return ToSqlUtils.getCreateFunctionSql(frontend_.getFunctions(
        params.category, params.db, params.pattern, true));
  }

  /**
   * Gets all roles
   */
  public byte[] getRoles(byte[] showRolesParams) throws ImpalaException {
    TShowRolesParams params = new TShowRolesParams();
    JniUtil.deserializeThrift(protocolFactory_, params, showRolesParams);
    TShowRolesResult result = new TShowRolesResult();

    List<Role> roles = Lists.newArrayList();
    if (params.isIs_show_current_roles() || params.isSetGrant_group()) {
      User user = new User(params.getRequesting_user());
      Set<String> groupNames;
      if (params.isIs_show_current_roles()) {
        groupNames = frontend_.getAuthzChecker().getUserGroups(user);
      } else {
        Preconditions.checkState(params.isSetGrant_group());
        groupNames = Sets.newHashSet(params.getGrant_group());
      }
      for (String groupName: groupNames) {
        roles.addAll(frontend_.getCatalog().getAuthPolicy().getGrantedRoles(groupName));
      }
    } else {
      Preconditions.checkState(!params.isIs_show_current_roles());
      roles = frontend_.getCatalog().getAuthPolicy().getAllRoles();
    }

    result.setRole_names(Lists.<String>newArrayListWithExpectedSize(roles.size()));
    for (Role role: roles) {
      result.getRole_names().add(role.getName());
    }

    Collections.sort(result.getRole_names());
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  public byte[] getRolePrivileges(byte[] showGrantRolesParams) throws ImpalaException {
    TShowGrantRoleParams params = new TShowGrantRoleParams();
    JniUtil.deserializeThrift(protocolFactory_, params, showGrantRolesParams);
    TResultSet result = frontend_.getCatalog().getAuthPolicy().getRolePrivileges(
        params.getRole_name(), params.getPrivilege());
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Executes a HiveServer2 metadata operation and returns a TResultSet
   */
  public byte[] execHiveServer2MetadataOp(byte[] metadataOpsParams)
      throws ImpalaException {
    TMetadataOpRequest params = new TMetadataOpRequest();
    JniUtil.deserializeThrift(protocolFactory_, params, metadataOpsParams);
    TResultSet result = frontend_.execHiveServer2MetadataOp(params);

    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  public void setCatalogInitialized() {
    frontend_.getCatalog().setIsReady();
  }

  // Caching this saves ~50ms per call to getHadoopConfigAsHtml
  private static final Configuration CONF = new Configuration();

  /**
   * Returns a string of all loaded Hadoop configuration parameters as a table of keys
   * and values. If asText is true, output in raw text. Otherwise, output in html.
   */
  public byte[] getAllHadoopConfigs() throws ImpalaException {
    Map<String, String> configs = Maps.newHashMap();
    for (Map.Entry<String, String> e: CONF) {
      configs.put(e.getKey(), e.getValue());
    }
    TGetAllHadoopConfigsResponse result = new TGetAllHadoopConfigsResponse();
    result.setConfigs(configs);
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns the corresponding config value for the given key as a serialized
   * TGetHadoopConfigResponse. If the config value is null, the 'value' field in the
   * thrift response object will not be set.
   */
  public byte[] getHadoopConfig(byte[] serializedRequest) throws ImpalaException {
    TGetHadoopConfigRequest request = new TGetHadoopConfigRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, serializedRequest);
    TGetHadoopConfigResponse result = new TGetHadoopConfigResponse();
    result.setValue(CONF.get(request.getName()));
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Creates a new delegation token.
   */
  public byte[] getDelegationToken(byte[] serializedRequest) throws ImpalaException {
    TGetDelegationTokenRequest request = new TGetDelegationTokenRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, serializedRequest);
    TGetDelegationTokenResponse result = new TGetDelegationTokenResponse();
    try {
      DelegationTokenManager.DelegationToken token =
          DelegationTokenManager.instance().getToken(
              new User(request.owner).getShortName(), request.renewer, request.user);
      result.identifier = token.identifier;
      result.password = token.password;
      result.token = ByteBuffer.wrap(token.token);
    } catch (IOException e) {
      throw new AuthenticationException("Could not get delegation token", e);
    }

    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Cancels the delegation token.
   */
  public void cancelDelegationToken(byte[] serializedRequest) throws ImpalaException {
    TCancelDelegationTokenRequest request = new TCancelDelegationTokenRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, serializedRequest);
    try {
      byte[] token = new byte[request.token.remaining()];
      request.token.get(token);
      DelegationTokenManager.instance().cancelToken(
          new User(request.user).getShortName(), token);
    } catch (IOException e) {
      throw new AuthenticationException("Could not cancel delegation token", e);
    }
  }

  /**
   * Renews the delegation token.
   */
  public void renewDelegationToken(byte[] serializedRequest) throws ImpalaException {
    TRenewDelegationTokenRequest request = new TRenewDelegationTokenRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, serializedRequest);
    try {
      byte[] token = new byte[request.token.remaining()];
      request.token.get(token);
      DelegationTokenManager.instance().renewToken(
          new User(request.user).getShortName(), token);
    } catch (IOException e) {
      throw new AuthenticationException("Could not renew delegation token", e);
    }
  }

  /**
   * Retrieves the user and password (stored in the server) for this token.
   */
  public byte[] retrieveUserAndPassword(byte[] serializedRequest) throws ImpalaException {
    TRetrieveUserAndPasswordRequest request = new TRetrieveUserAndPasswordRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, serializedRequest);
    TRetrieveUserAndPasswordResponse result = new TRetrieveUserAndPasswordResponse();
    try {
      DelegationTokenManager.UserPassword userPw =
          DelegationTokenManager.instance().retrieveUserPassword(request.identifier);
      result.user = userPw.user;
      result.password = userPw.password;
    } catch (IOException e) {
      throw new AuthenticationException("Could not get delegation token", e);
    }

    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Return the master key corresponds to the sequence number in 'serializedRequest'
   */
  public byte[] getMasterKey(byte[] serializedRequest) throws ImpalaException {
    TGetMasterKeyRequest request = new TGetMasterKeyRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, serializedRequest);
    TGetMasterKeyResponse result = new TGetMasterKeyResponse();
    try {
      Pair<Integer, String> pair =
          DelegationTokenManager.instance().getMasterKey(request.seq);
      result.seq = pair.getLeft();
      result.key = pair.getRight();
      return new TSerializer(protocolFactory_).serialize(result);
    } catch (IOException e) {
      throw new InternalException(e.getMessage(), e);
    } catch (TException e) {
      throw new InternalException(e.getMessage(), e);
    }
  }

  /**
   * Returns an error string describing all configuration issues. If no config issues are
   * found, returns an empty string.
   */
  public String checkConfiguration() {
    StringBuilder output = new StringBuilder();
    output.append(checkLogFilePermission());
    output.append(checkFileSystem(CONF));
    if (isDataNode()) {
      output.append(checkShortCircuitRead(CONF));
    } else {
      LOG.info("Service is not running on a DataNode. Skippping short circuit read "
          + "configuration checks.");
    }
    output.append(checkBlockLocationTracking(CONF));
    return output.toString();
  }

  /**
   * Return true if it is recordservice
   */
  private boolean isRecordService() {
    return runningPlanner_ || runningWorker_;
  }

  /**
   * Returns an empty string if Impala has permission to write to FE log files. If not,
   * returns an error string describing the issues.
   */
  private String checkLogFilePermission() {
    org.apache.log4j.Logger l4jRootLogger = org.apache.log4j.Logger.getRootLogger();
    Enumeration appenders = l4jRootLogger.getAllAppenders();
    while (appenders.hasMoreElements()) {
      Appender appender = (Appender) appenders.nextElement();
      if (appender instanceof FileAppender) {
        if (((FileAppender) appender).getFile() == null) {
          // If Impala does not have permission to write to the log file, the
          // FileAppender will fail to initialize and logFile will be null.
          // Unfortunately, we can't get the log file name here.
          return "Impala does not have permission to write to the log file specified " +
              "in log4j.properties.";
        }
      }
    }
    return "";
  }

  /**
   * Return an empty string if short circuit read is properly enabled. If not, return an
   * error string describing the issues.
   */
  private String checkShortCircuitRead(Configuration conf) {
    StringBuilder output = new StringBuilder();
    String errorMessage = "ERROR: short-circuit local reads is disabled because\n";
    String prefix = "  - ";
    StringBuilder errorCause = new StringBuilder();

    // dfs.domain.socket.path must be set properly
    String domainSocketPath = conf.getTrimmed(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_DEFAULT);
    if (domainSocketPath.isEmpty()) {
      errorCause.append(prefix);
      errorCause.append(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY);
      errorCause.append(" is not configured.\n");
    } else {
      // The socket path parent directory must be readable and executable.
      File socketFile = new File(domainSocketPath);
      File socketDir = socketFile.getParentFile();
      if (socketDir == null || !socketDir.canRead() || !socketDir.canExecute()) {
        errorCause.append(prefix);
        errorCause.append("Impala cannot read or execute the parent directory of ");
        errorCause.append(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY);
        errorCause.append("\n");
      }
    }

    // dfs.client.read.shortcircuit must be set to true.
    if (!conf.getBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY,
        DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_DEFAULT)) {
      errorCause.append(prefix);
      errorCause.append(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY);
      errorCause.append(" is not enabled.\n");
    }

    // dfs.client.use.legacy.blockreader.local must be set to false
    if (conf.getBoolean(DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL,
        DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT)) {
      errorCause.append(prefix);
      errorCause.append(DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL);
      errorCause.append(" should not be enabled.\n");
    }

    if (errorCause.length() > 0) {
      output.append(errorMessage);
      output.append(errorCause);
    }

    return output.toString();
  }

  /**
   * Return an empty string if block location tracking is properly enabled. If not,
   * return an error string describing the issues.
   */
  private String checkBlockLocationTracking(Configuration conf) {
    StringBuilder output = new StringBuilder();
    String errorMessage = "ERROR: block location tracking is not properly enabled " +
        "because\n";
    String prefix = "  - ";
    StringBuilder errorCause = new StringBuilder();
    if (!conf.getBoolean(DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED,
        DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED_DEFAULT)) {
      errorCause.append(prefix);
      errorCause.append(DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED);
      errorCause.append(" is not enabled.\n");
    }

    // dfs.client.file-block-storage-locations.timeout.millis should be >= 10 seconds
    int dfsClientFileBlockStorageLocationsTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS,
        DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS_DEFAULT);
    if (dfsClientFileBlockStorageLocationsTimeoutMs <
        MIN_DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS) {
      errorCause.append(prefix);
      errorCause.append(DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS);
      errorCause.append(" is too low. It should be at least 10 seconds.\n");
    }

    if (errorCause.length() > 0) {
      output.append(errorMessage);
      output.append(errorCause);
    }

    return output.toString();
  }

  /**
   * Return an empty string if the default FileSystem configured in CONF refers to a
   * DistributedFileSystem and Impala can list the root directory "/". Otherwise,
   * return an error string describing the issues.
   */
  private String checkFileSystem(Configuration conf) {
    try {
      FileSystem fs = FileSystem.get(CONF);
      if (!(fs instanceof DistributedFileSystem)) {
        return "Unsupported default filesystem. The default filesystem must be " +
            "a DistributedFileSystem but the configured default filesystem is " +
            fs.getClass().getSimpleName() + ". " +
            CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY +
            " (" + CONF.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) + ")" +
            " might be set incorrectly.";
      }
    } catch (IOException e) {
      return "couldn't retrieve FileSystem:\n" + e.getMessage();
    }

    try {
      FileSystemUtil.getTotalNumVisibleFiles(new Path("/"));
    } catch (IOException e) {
      return "Could not read the HDFS root directory at " +
          CONF.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) +
          ". Error was: \n" + e.getMessage();
    }
    return "";
  }

  /**
   * return true if the current node also acts as a DataNode.
   * TODO: Find a better way to check the DataNode process.
   */
  private boolean isDataNode() {
    Process process = null;
    try {
      String[] cmd = {"/usr/bin/env", "-c", "ps -ef | grep hadoop | grep -P datanode"};
      process = new ProcessBuilder(cmd).start();
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.contains(cmd[2])) {
          return true;
        }
      }
    } catch (Exception e) {
      LOG.error("Got exception when checking the DataNode Process", e);
    } finally {
      if (process != null) {
        process.destroy();
      }
    }
    return false;
  }
}
