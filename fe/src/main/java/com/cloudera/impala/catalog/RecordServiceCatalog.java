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

import java.util.List;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.cloudera.impala.authorization.SentryConfig;
import com.cloudera.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.thrift.TTableName;
import com.cloudera.impala.thrift.TUniqueId;
import com.google.common.base.Preconditions;

/**
 * Catalog for recordservice. RecordserviceCatalog only uses metadata cache in getDb()
 * for buildinsDb_, and always reloads table metadata for each getTable request.
 * It also maintains a in memory db for tmp tables from path requests. The db and
 * the tmp tables do not exist in HMS.
 * TODO: Update dbCache_ with per query snapshot in a proper way.
 * TODO: Update authorization policy from Sentry.
 */
public class RecordServiceCatalog extends CatalogServiceCatalog{
  private static final Logger LOG = Logger.getLogger(RecordServiceCatalog.class);

  // The database used for temporary tables used for RecordService path request.
  private final String rsTmpDb_;

  public RecordServiceCatalog(int numLoadingThreads, String rsTmpDb,
      SentryConfig sentryConfig, TUniqueId catalogServiceId) {
    super(false, numLoadingThreads, sentryConfig, catalogServiceId);
    rsTmpDb_ = rsTmpDb;
    addDb(rsTmpDb_);
  }

  /**
   * Returns the Table object for the given dbName/tableName. If the dbName matches
   * the in memory db used to store the tmp tables for path requests, this
   * will skip the HMS and fetch the table from the db instead. Otherwise, it goes
   * to the HMS and load the table.
   * Returns null if the table does not exist. Throws a TableLoadingException if the
   * table's metadata was not able to be loaded successfully and
   * DatabaseNotFoundException if the parent database does not exist.
   * Unlike CatalogServiceCatalog or ImpaladCatalog, it never uses cache, but reloads
   * the table for each query.
   */
  @Override
  public Table getTable(String dbName, String tableName) throws CatalogException {
    Table table;
    if (rsTmpDb_.equals(dbName)) {
      // This is a tmp table request. Return the table in the tmp db.
      table = getDb(dbName).getTable(tableName);
      if (table == null) {
        throw new TableLoadingException("Failed to load temp table '" + tableName + "'");
      }
      return table;
    }

    MetaStoreClient msClient = getMetaStoreClient();
    // Return null if table does not exists in metadata.
    try {
      if (!msClient.getHiveClient().tableExists(dbName, tableName)) return null;
    } catch (UnknownDBException e) {
      // The parent database does not exist in the metastore.
      throw new DatabaseNotFoundException(e.toString());
    } catch (TException e) {
      // Error executing tableExists() metastore call.
      throw new TableLoadingException(e.toString());
    } finally {
      msClient.release();
      msClient = null;
    }

    // Create incompleteTable.
    table = IncompleteTable.createUninitializedTable(TableId.createInvalidId(),
        new Db(dbName, this), tableName);

    // Load table via load mgr.
    TableLoadingMgr.LoadRequest loadReq = tableLoadingMgr_.load(new TTableName(
        dbName, tableName), table.getDb());
    Preconditions.checkNotNull(loadReq);

    try {
      table = loadReq.get();
    } finally {
      loadReq.close();
    }

    if (table.isLoaded() && table instanceof IncompleteTable) {
      // If there were problems loading this table's metadata, throw an exception
      // when it is accessed.
      ImpalaException cause = ((IncompleteTable) table).getCause();
      if (cause instanceof TableLoadingException) throw (TableLoadingException) cause;
      throw new TableLoadingException("Missing metadata for table: " + tableName, cause);
    }

    return table;
  }

  /**
   * Returns true if the table and the database exist in HMS. Otherwise return false.
   */
  @Override
  public boolean containsTable(String dbName, String tblName) {
    MetaStoreClient msClient = getMetaStoreClient();
    try {
      return msClient.getHiveClient().tableExists(dbName, tblName);
    } catch (UnknownDBException e) {
      LOG.error("The parent database " + dbName + " does not exist in the metastore", e);
      return false;
    } catch (TException e) {
      LOG.error("Error executing tableExists() metastore call for " + dbName + "."
          + tblName, e);
      return false;
    } finally {
      msClient.release();
    }
  }

  /**
   * Gets the Db object from the Catalog using a case-insensitive lookup on the name.
   * Creates and returns a new Db if db is not in dbCache_ but exists in HMS.
   * Returns null if no matching database is found.
   */
  @Override
  public Db getDb(String dbName) {
    Preconditions.checkState(dbName != null && !dbName.isEmpty(),
        "Null or empty database name given as argument to CatalogRecordService.getDb");

    // Get db from dbCache_ only for builtinsDb_.
    if (dbCache_.get().containsKey(dbName.toLowerCase())) {
      return dbCache_.get().get(dbName.toLowerCase());
    }

    // Check if db exists in HMS.
    MetaStoreClient msClient = getMetaStoreClient();
    try {
      List<String> listOfDb = msClient.getHiveClient()
          .getDatabases(dbName.toLowerCase());
      for (String db : listOfDb) {
        if (db.equals(dbName)) {
          return new Db(dbName, this);
        }
      }
    } catch (MetaException e) {
      LOG.info("Got TException during getDatabases from HMS:" + e);
    } finally {
      msClient.release();
    }

    return null;
  }

  /**
   * No-op. This catalog does not do any caching.
   */
  @Override
  public void reset() throws CatalogException {
    return;
  }
}
