/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.impala.service;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TMembershipUpdate;
import com.cloudera.impala.thrift.TMembershipUpdateType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Wrapper around curator to manage zookeeper sessions. This class also
 * handles membership.
 */
public class ZooKeeperSession implements Closeable {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ZooKeeperSession.class.getName());

  // Location of zookeeper quorem.
  public static final String ZOOKEEPER_CONNECTION_STRING_CONF =
      "recordservice.zookeeper.connectString";
  public static final String ZOOKEEPER_CONNECT_TIMEOUTMILLIS_CONF =
      "recordservice..zookeeper.connectTimeoutMillis";

  // Root zookeeper directory.
  public static final String ZOOKEEPER_ZNODE_CONF =
      "recordservice.zookeeper.znode";
  public static final String ZOOKEEPER_ZNODE_DEFAULT = "/recordservice";

  // Acl to use when creating znodes
  public static final String ZOOKEEPER_STORE_ACL_CONF =
      "recordservice.zookeeper.acl";

  // Kerberos settings.
  public static final String RECORD_SERVICE_SERVER_PRINCIPAL_CONF =
      "recordservice.server.kerberos.principal";
  public static final String RECORD_SERVICE_SERVER_KEY_TAB_CONF =
      "recordservice.server.kerberos.keytab.file";

  // Zookeeper directory locations
  private static final String PLANNER_MEMBERSHIP_ZNODE = "planners";
  private static final String WORKER_MEMBERSHIP_ZNODE = "workers";

  // ZooKeeper property name to pick the correct JAAS conf section
  private static final String SASL_LOGIN_CONTEXT_NAME = "RecordServiceZooKeeperClient";

  // Zookeeper connection string (host/port of quorem).
  private final String zkConnectString_;

  // Session timeout.
  private final int connectTimeoutMillis_;

  // Root znode directory.
  private String rootNode_ = "";

  // Current Zookeeper session.
  private volatile CuratorFramework zkSession_;

  // Watcher for the planner membership znode.
  private PathChildrenCache plannerMembership_;

  // Watcher of the worker membership znode.
  private PathChildrenCache workerMembership_;

  // Planner membership. Only maintained for planners. (runningPlanner_ = true)
  // Access to this needs to be thread safe. If this and zkSession_ need to be
  // locked, lock zkSession_ first.
  private final Set<String> planners_ = Sets.newHashSet();

  // Worker membership. Only maintained for planners. (runningPlanner_ = true)
  // Access to this needs to be thread safe. If this and zkSession_ need to be
  // locked, lock zkSession_ first.
  private final Set<String> workers_ = Sets.newHashSet();

  // List of callbacks to invoke when the zk sessions is (re) created.
  private final List<NewSessionCb> newSessionsCbs_ = Lists.newArrayList();

  // ACLs to use when creating new znodes.
  private List<ACL> newNodeAcl_ = Arrays.asList(new ACL(Perms.ALL, Ids.AUTH_IDS));

  // ID for this service.
  private final String id_;

  // Planner and worker ports. Greater than 0 if set.
  private final int plannerPort_;
  private final int workerPort_;

  /**
   * ACLProvider permissions will be used in case parent dirs need to be created
   */
  private final ACLProvider aclDefaultProvider_ = new ACLProvider() {
    @Override
    public List<ACL> getDefaultAcl() {
      return newNodeAcl_;
    }
    @Override
    public List<ACL> getAclForPath(String path) {
      return getDefaultAcl();
    }
  };

  /**
   * Connects to zookeeper and handles maintaining membership.
   * Note: this can only be called when either planner or worker is running.
   * @param conf
   * @param id - The ID for this server. This should be unique among
   *    all instances of the service.
   * @param plannerPort - If greater than 0, running the planner service.
   * @param workerPort - If greater than 0, running the worker service.
   */
  public ZooKeeperSession(Configuration conf, String id, int plannerPort,
      int workerPort) throws IOException {
    id_ = id;
    plannerPort_ = plannerPort;
    workerPort_ = workerPort;
    Preconditions.checkArgument(runningPlanner() || runningWorker(),
        "ZooKeeperSession cannot be initialized when neither" +
            " planner nor worker is running. ");

    zkConnectString_ = conf.get(ZOOKEEPER_CONNECTION_STRING_CONF);
    if (zkConnectString_ == null || zkConnectString_.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Zookeeper connect string has to be specified through "
          + ZOOKEEPER_CONNECTION_STRING_CONF);
    }
    LOGGER.info("Connecting to zookeeper at: " + zkConnectString_ + " with id: " + id_);

    connectTimeoutMillis_ =
        conf.getInt(ZOOKEEPER_CONNECT_TIMEOUTMILLIS_CONF,
            CuratorFrameworkFactory.builder().getConnectionTimeoutMs());

    String aclStr = conf.get(ZOOKEEPER_STORE_ACL_CONF, null);
    LOGGER.info("Zookeeper acl: " + aclStr);
    if (StringUtils.isNotBlank(aclStr)) newNodeAcl_ = parseACLs(aclStr);

    rootNode_ = conf.get(ZOOKEEPER_ZNODE_CONF, ZOOKEEPER_ZNODE_DEFAULT);
    LOGGER.info("Zookeeper root: " + rootNode_);

    // Install the JAAS Configuration for the runtime
    setupJAASConfig(conf);
    initMembershipPaths();
  }

  /**
   * Closes the underlying ZK session. This should be called if the server is stopped
   * to remove this node from ZK more quickly (otherwise it will timeout eventually).
   */
  public void close() throws IOException {
    if (plannerMembership_ != null) {
      plannerMembership_.close();
      synchronized (planners_) {
        planners_.clear();
      }
    }
    if (workerMembership_ != null) {
      workerMembership_.close();
      synchronized (workers_) {
        workers_.clear();
      }
    }
    if (zkSession_ != null) zkSession_.close();
  }

  /**
   * Callback that is invoked when a sessions restarts.
   */
  public interface NewSessionCb {
    void newSession() throws IOException;
  }

  /**
   * Adds a callback to be invoked when a new ZK session is created.
   */
  public void addNewSessionCb(NewSessionCb cb) {
    synchronized (newSessionsCbs_) {
      newSessionsCbs_.add(cb);
    }
  }

  /**
   * Returns the current Curator session, reconnecting if necessary. As part of this,
   * if the connection is re(created), membership is updated accordingly.
   */
  public CuratorFramework getSession() throws IOException {
    if (zkSession_ == null || zkSession_.getState() == CuratorFrameworkState.STOPPED) {
      boolean recreatedSession = false;
      synchronized (this) {
        if (zkSession_ == null || zkSession_.getState() == CuratorFrameworkState.STOPPED) {
          zkSession_ = CuratorFrameworkFactory.builder()
              .connectString(zkConnectString_)
              .connectionTimeoutMs(connectTimeoutMillis_)
              .aclProvider(aclDefaultProvider_)
              .retryPolicy(new ExponentialBackoffRetry(1000, 3))
              .build();
          zkSession_.start();
          recreatedSession = true;
        }
      }
      // Just started a new session, register membership.
      if (recreatedSession) {
        if (runningPlanner()) registerMembership(zkSession_, true);
        if (runningWorker()) registerMembership(zkSession_, false);

        synchronized (newSessionsCbs_) {
          for (NewSessionCb cb: newSessionsCbs_) {
            cb.newSession();
          }
        }
      }
    }
    return zkSession_;
  }

  public String rootZnode() { return rootNode_; }
  public List<ACL> newNodeAcl() { return newNodeAcl_; }

  /**
   * Returns a path relative (appended to) the root znode path.
   */
  public String getRelativePath(String path) {
    return rootNode_ + "/" + path;
  }

  /**
   * Returns the set of planner or workers, depending on whether 'planner' is true
   * or false. Only callable for planner services.
   */
  public Set<String> getMembership(boolean planner) {
    if (!runningPlanner()) {
      // TODO: we can either always listen to the membership or just get the list on
      // demand for worker-only services. Currently, there is no use case for this.
      throw new IllegalStateException("Can only call when running planner.");
    }
    Set<String> members = planner ? planners_ : workers_;
    synchronized (members) {
      return ImmutableSet.copyOf(members);
    }
  }

  /**
   * Returns the size for planners or workers, depending on whether 'planner' is
   * true or false.
   */
  public int getMembershipSize(boolean planner) {
    return planner ? planners_.size() : workers_.size();
  }

  /**
   * Create a path if it does not already exist ("mkdir -p")
   * @param path string with '/' separator
   * @param acl list of ACL entries
   */
  public void ensurePath(String path, List<ACL> acl) throws IOException {
    try {
      CuratorFramework zk = getSession();
      String node = zk.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
          .withACL(acl).forPath(path);
      LOGGER.info("Created path: {} ", node);
    } catch (KeeperException.NodeExistsException e) {
      // node already exists
      LOGGER.debug("ZNode already exists: {} ", path);
    } catch (Exception e) {
      throw new IOException("Error creating path " + path, e);
    }
  }

  /**
   * Deletes the znode at 'path'. Idempotent.
   */
  public void zkDelete(String path) throws IOException {
    CuratorFramework zk = getSession();
    try {
      zk.delete().forPath(path);
    } catch (KeeperException.NoNodeException ex) {
      // already deleted
    } catch (Exception e) {
      throw new IOException("Error deleting " + path, e);
    }
  }

  /**
   * Lists all the children in 'path'.
   */
  public List<String> zkGetChildren(String path) throws IOException {
    CuratorFramework zk = getSession();
    try {
      return zk.getChildren().forPath(path);
    } catch (Exception e) {
      throw new IOException("Error getting children for " + path, e);
    }
  }

  /**
   * Returns the data at 'nodePath'. Returns null if there is no data (but
   * the path exists).
   */
  public byte[] zkGetData(String nodePath) throws IOException {
    CuratorFramework zk = getSession();
    try {
      return zk.getData().forPath(nodePath);
    } catch (KeeperException.NoNodeException ex) {
      return null;
    } catch (Exception e) {
      throw new IOException("Error reading " + nodePath, e);
    }
  }

  /**
   * Updates the worker or planner membership, including calling into the BE.
   */
  private void updateMembership(TMembershipUpdateType type,
      String member, boolean planner) throws InternalException {
    PathChildrenCache membership = planner ? plannerMembership_ : workerMembership_;
    Set<String> members = planner ? planners_ : workers_;
    synchronized (members) {
      TMembershipUpdate update =
          new TMembershipUpdate(planner, type, new ArrayList<String>());
      switch (type) {
      case ADD:
        Preconditions.checkNotNull(member);
        members.add(member);
        update.membership.add(member);
        break;
      case REMOVE:
        Preconditions.checkNotNull(member);
        members.remove(member);
        update.membership.add(member);
        break;
      case FULL_LIST:
        Preconditions.checkState(member == null);
        members.clear();
        for (ChildData d: membership.getCurrentData()) {
          members.add(d.getPath());
        }
        update.membership.addAll(members);
        break;
      default:
        Preconditions.checkState(false);
      }
      FeSupport.UpdateMembership(update);
    }
  }

  /**
   * Adds this service in the planner and/or membership directory. This is done
   * in Zookeeper by creating a ephemeral znode with 'id'. ephemeral means that
   * if the session closes (zookeeper manages timeouts), the znode is deleted.
   */
  private void registerMembership(CuratorFramework zk, boolean planner)
      throws IOException {
    if (planner) {
      // This is the planner meaning we also want to listen to the membership.
      initMembership(zk, true);
      initMembership(zk, false);
    }

    String serviceId = id_ + ":" + (planner ? plannerPort_ : workerPort_);
    String path = getRelativePath(
        (planner ? PLANNER_MEMBERSHIP_ZNODE : WORKER_MEMBERSHIP_ZNODE)
            + "/" + serviceId);
    // First delete the current path. This is required for correctness. For example, if
    // this service is restarted, the previous znode might still be there (hasn't timed
    // out yet) and this function would not be able to create the znode with this
    // session.
    zkDelete(path);
    try {
      String node = zk.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
        .withACL(newNodeAcl_).forPath(path);
      LOGGER.info("Created path: {} ", node);
    } catch (Exception e) {
      throw new IOException("Could not create znode at " + path, e);
    }
  }

  /**
   * Initiate membership for planner or worker, depending on whether 'planner' is
   * true or false. This reconstruct the membership information based on the current
   * state, and add listener for future membership updates.
   */
  private void initMembership(CuratorFramework zk, final boolean planner)
      throws IOException {
    PathChildrenCache membership = planner ? plannerMembership_ : workerMembership_;
    Set<String> members = planner ? planners_ : workers_;
    String znode = planner ? PLANNER_MEMBERSHIP_ZNODE : WORKER_MEMBERSHIP_ZNODE;
    synchronized (zkSession_) {
      if (membership != null) membership.close();
      membership = new PathChildrenCache(zk, getRelativePath(znode), true);
      if (planner) plannerMembership_ = membership;
      else workerMembership_ = membership;
      try {
        Preconditions.checkState(members.isEmpty());
        membership.start(StartMode.BUILD_INITIAL_CACHE);
        updateMembership(TMembershipUpdateType.FULL_LIST, null, planner);
      } catch (Exception e) {
        throw new IOException(
          "Could not watch " + (planner ? "planner" : "worker") + " membership.", e);
      }
    }

    // Add a listener to the worker or planner directory for updates.
    membership.getListenable().addListener(new PathChildrenCacheListener() {
        @Override
        public void childEvent(CuratorFramework zk, PathChildrenCacheEvent arg)
          throws Exception {
          synchronized (zkSession_) {
            // This session is no longer the active one, ignore this update.
            if (zk != zkSession_) return;
            switch (arg.getType()) {
            case CHILD_ADDED:
              updateMembership(
                TMembershipUpdateType.ADD, arg.getData().getPath(), planner);
              break;
            case CHILD_REMOVED:
              updateMembership(
                TMembershipUpdateType.REMOVE, arg.getData().getPath(), planner);
              break;

            case INITIALIZED:
            default:
              // TODO: what do the other types mean? Should be safe to just
              // reconstruct.
              LOGGER.info("Found unexpected event type {}. Reconstruct the membership.",
                  arg.getType());
              updateMembership(TMembershipUpdateType.FULL_LIST, null, planner);
              break;
            }
          }
        }
      });
  }

  /**
   * Initializes the paths for storing membership. This creates the directories
   * for planners and workers.
   */
  private void initMembershipPaths() throws IOException {
    ensurePath(getRelativePath(PLANNER_MEMBERSHIP_ZNODE), newNodeAcl_);
    ensurePath(getRelativePath(WORKER_MEMBERSHIP_ZNODE), newNodeAcl_);
  }

  /**
   * Whether this is running as a planner service.
   */
  private boolean runningPlanner() { return plannerPort_ > 0; }

  /**
   * Whether this is running as a worker service.
   */
  private boolean runningWorker() { return workerPort_ > 0; }

  /**
   * Parse comma separated list of ACL entries to secure generated nodes, e.g.
   * <code>sasl:recordservice/host1@MY.DOMAIN:cdrwa,</code>
   */
  private static List<ACL> parseACLs(String aclString) {
    String[] aclComps = StringUtils.splitByWholeSeparator(aclString, ",");
    List<ACL> acl = new ArrayList<ACL>(aclComps.length);
    for (String a : aclComps) {
      if (StringUtils.isBlank(a)) {
         continue;
      }
      a = a.trim();
      // from ZooKeeperMain private method
      int firstColon = a.indexOf(':');
      int lastColon = a.lastIndexOf(':');
      if (firstColon == -1 || lastColon == -1 || firstColon == lastColon) {
         LOGGER.error(a + " does not have the form scheme:id:perm");
         continue;
      }
      ACL newAcl = new ACL();
      newAcl.setId(new Id(a.substring(0, firstColon), a.substring(
          firstColon + 1, lastColon)));
      newAcl.setPerms(getPermFromString(a.substring(lastColon + 1)));
      acl.add(newAcl);
    }
    return acl;
  }

  /**
   * Parse ACL permission string, from ZooKeeperMain private method
   */
  private static int getPermFromString(String permString) {
    int perm = 0;
    for (int i = 0; i < permString.length(); i++) {
      switch (permString.charAt(i)) {
        case 'r':
          perm |= ZooDefs.Perms.READ;
          break;
        case 'w':
          perm |= ZooDefs.Perms.WRITE;
          break;
        case 'c':
          perm |= ZooDefs.Perms.CREATE;
          break;
        case 'd':
          perm |= ZooDefs.Perms.DELETE;
          break;
        case 'a':
          perm |= ZooDefs.Perms.ADMIN;
          break;
        default:
          LOGGER.error("Unknown perm type: " + permString.charAt(i));
      }
    }
    return perm;
  }

  /**
   * A JAAS configuration for ZooKeeper clients intended to use for SASL
   * Kerberos.
   */
  private static class JaasConfiguration
      extends javax.security.auth.login.Configuration {
    // Current installed Configuration
    private final javax.security.auth.login.Configuration baseConfig =
        javax.security.auth.login.Configuration.getConfiguration();
    private final String loginContextName;
    private final String principal;
    private final String keyTabFile;

    public JaasConfiguration(String hiveLoginContextName,
        String principal, String keyTabFile) {
      this.loginContextName = hiveLoginContextName;
      this.principal = principal;
      this.keyTabFile = keyTabFile;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      if (loginContextName.equals(appName)) {
        Map<String, String> krbOptions = new HashMap<String, String>();
        krbOptions.put("doNotPrompt", "true");
        krbOptions.put("storeKey", "true");
        krbOptions.put("useKeyTab", "true");
        krbOptions.put("principal", principal);
        krbOptions.put("keyTab", keyTabFile);
        krbOptions.put("refreshKrb5Config", "true");
        AppConfigurationEntry hiveZooKeeperClientEntry = new AppConfigurationEntry(
            KerberosUtil.getKrb5LoginModuleName(),
            LoginModuleControlFlag.REQUIRED, krbOptions);
        return new AppConfigurationEntry[] { hiveZooKeeperClientEntry };
      }
      // Try the base config
      if (baseConfig != null) {
        return baseConfig.getAppConfigurationEntry(appName);
      }
      return null;
    }
  }

  /**
   * Setup configuration to connect to Zookeeper using kerberos.
   */
  private void setupJAASConfig(Configuration conf) throws IOException {
    String principal = conf.get(RECORD_SERVICE_SERVER_PRINCIPAL_CONF);
    if (principal != null) {
      String keytab = conf.get(RECORD_SERVICE_SERVER_KEY_TAB_CONF);
      if (keytab == null || keytab.trim().isEmpty()) {
        throw new IOException("Keytab must be set to connect using kerberos.");
      }
      System.setProperty(
          ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY, SASL_LOGIN_CONTEXT_NAME);
      principal = SecurityUtil.getServerPrincipal(principal, "0.0.0.0");
      JaasConfiguration jaasConf =
          new JaasConfiguration(SASL_LOGIN_CONTEXT_NAME, principal, keytab);
      // Install the Configuration in the runtime.
      javax.security.auth.login.Configuration.setConfiguration(jaasConf);
    }
  }
}
