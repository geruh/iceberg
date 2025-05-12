/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.catalog;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

import com.google.errorprone.annotations.FormatMethod;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.UpdateStatistics;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;

public class BaseCatalogTransaction implements CatalogTransaction {
  private final Catalog origin;
  private final IsolationLevel isolationLevel;
  private final Map<TableIdentifier, Transaction> txByTable;
  private boolean hasCommitted = false;

  public BaseCatalogTransaction(Catalog catalog, IsolationLevel isolationLevel) {
    Preconditions.checkArgument(null != catalog, "Invalid origin catalog: null");
    Preconditions.checkArgument(null != isolationLevel, "Invalid isolation level: null");
    Preconditions.checkArgument(
        catalog instanceof SupportsCatalogTransactions,
        "Origin catalog does not support catalog transactions");
    this.origin = catalog;
    this.isolationLevel = isolationLevel;
    this.txByTable = Maps.newHashMap();
  }

  @Override
  public IsolationLevel isolationLevel() {
    return isolationLevel;
  }

  @Override
  public Catalog asCatalog() {
    return new StaticCatalog();
  }

  private Transaction txForTable(Table table) {
    return txByTable.computeIfAbsent(
        identifierWithoutCatalog(table.name()),
        k -> Transactions.newTransaction(table.name(), ((HasTableOperations) table).operations()));
  }

  private TableIdentifier identifierWithoutCatalog(String tableWithCatalog) {
    if (tableWithCatalog.startsWith(origin.name())) {
      return TableIdentifier.parse(tableWithCatalog.replace(origin.name() + ".", ""));
    }
    return TableIdentifier.parse(tableWithCatalog);
  }

  private void checkReadWriteConflicts() {
    for (Map.Entry<TableIdentifier, Transaction> tableId : txByTable.entrySet()) {
      TableIdentifier identifier = tableId.getKey();
      BaseTransaction tableTxn = (BaseTransaction) tableId.getValue();
      boolean isCreate = tableTxn.startMetadata() == null;
      if (isCreate) {
        if (origin.tableExists(identifier)) {
          throw new SerializationConflictException(
              "Serializable isolation violation: table %s was created after transaction began",
              identifier);
        }
      } else {
        BaseTable latestTable = (BaseTable) origin.loadTable(identifier);
        if (!latestTable
            .operations()
            .current()
            .metadataFileLocation()
            .equals(tableTxn.startMetadata().metadataFileLocation())) {
          throw new SerializationConflictException(
              "Serializable isolation violation: table %s was modified after transaction began",
              identifier);
        }
      }
    }
  }

  @Override
  public void commitTransaction() {
    Preconditions.checkState(!hasCommitted, "Transaction has already committed changes");

    List<TableCommit> tableCommits = Lists.newArrayList();
    AtomicBoolean isRetry = new AtomicBoolean(false);
    try {
      Tasks.foreach(origin)
          .retry(COMMIT_NUM_RETRIES_DEFAULT)
          .exponentialBackoff(
              COMMIT_MIN_RETRY_WAIT_MS_DEFAULT,
              COMMIT_MAX_RETRY_WAIT_MS_DEFAULT,
              COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT,
              2.0)
          .onlyRetryOn(CommitFailedException.class)
          .run(
              catalog -> {
                if (isolationLevel() == IsolationLevel.SERIALIZABLE) {
                  checkReadWriteConflicts();
                }
                if (isRetry.get()) {
                  rebaseTransactions();
                }
                isRetry.set(true);
                tableCommits.clear();
                collectTableCommits(tableCommits);
                // Commit all changes atomically
                if (!tableCommits.isEmpty()) {
                  ((SupportsCatalogTransactions) catalog).multiTableCommit(tableCommits);
                }
              });
    } catch (CommitStateUnknownException e) {
      // Don't clean up, we don't know if commit succeeded
      throw e;
    } catch (RuntimeException e) {
      if (e instanceof CleanableFailure) {
        for (Transaction tx : txByTable.values()) {
          if (tx instanceof BaseTransaction
              && ((BaseTransaction) tx).underlyingOps().requireStrictCleanup()) {
            ((BaseTransaction) tx).cleanAllUpdates();
          }
        }
      }
      throw e;
    } finally {
      for (Transaction tx : txByTable.values()) {
        ((BaseTransaction) tx).cleanUpUncommittedFiles();
      }
      hasCommitted = true;
    }
  }

  private void collectTableCommits(List<TableCommit> tableCommits) {
    for (Map.Entry<TableIdentifier, Transaction> entry : txByTable.entrySet()) {
      if (entry.getValue() instanceof BaseTransaction) {
        BaseTransaction tx = (BaseTransaction) entry.getValue();
        if (tx.hasUpdates()) {
          tableCommits.add(
              TableCommit.create(entry.getKey(), tx.startMetadata(), tx.currentMetadata()));
        }
      }
    }
  }

  private Optional<Table> txTable(TableIdentifier identifier) {
    if (txByTable.containsKey(identifier)) {
      return Optional.ofNullable(txByTable.get(identifier).table());
    }
    return Optional.empty();
  }

  private void rebaseTransactions() {
    for (Transaction tx : txByTable.values()) {
      if (tx instanceof BaseTransaction) {
        // refresh the latest
        BaseTransaction baseTxn = (BaseTransaction) tx;
        baseTxn.applyUpdates(baseTxn.underlyingOps());
      }
    }
  }

  public class StaticCatalog implements Catalog {
    private Long pinnedSequenceNumber = null;

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      ensureSequencePinned();
      return origin.listTables(namespace);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      ensureSequencePinned();
      return origin.dropTable(identifier, purge);
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
      ensureSequencePinned();
      origin.renameTable(from, to);
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
      ensureSequencePinned();

      Table table =
          BaseCatalogTransaction.this
              .txTable(identifier)
              .orElseGet(
                  () ->
                      ((SupportsCatalogTransactions) origin)
                          .loadTable(identifier, pinnedSequenceNumber));
      Transaction ops =
          Transactions.newTransaction(table.name(), ((HasTableOperations) table).operations());
      txByTable.putIfAbsent(identifierWithoutCatalog(table.name()), ops);
      return new TransactionalTable(
          table, ((BaseTransaction.TransactionTable) ops.table()).operations());
    }

    @Override
    public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
      return new TransactionTableBuilder(identifier, schema);
    }

    private void ensureSequencePinned() {
      if (pinnedSequenceNumber == null) {
        pinnedSequenceNumber = ((SupportsCatalogTransactions) origin).catalogSequenceNumber();
      }
    }
  }

  private class TransactionalTable extends BaseTable {
    private final Table table;

    private TransactionalTable(Table table, TableOperations ops) {
      super(ops, table.name());
      this.table = table;
    }

    @Override
    public TableScan newScan() {
      return txForTable(table).table().newScan();
    }

    @Override
    public UpdateSchema updateSchema() {
      return txForTable(table).updateSchema();
    }

    @Override
    public UpdatePartitionSpec updateSpec() {
      return txForTable(table).updateSpec();
    }

    @Override
    public UpdateProperties updateProperties() {
      return txForTable(table).updateProperties();
    }

    @Override
    public ReplaceSortOrder replaceSortOrder() {
      return txForTable(table).replaceSortOrder();
    }

    @Override
    public UpdateLocation updateLocation() {
      return txForTable(table).updateLocation();
    }

    @Override
    public AppendFiles newAppend() {
      return txForTable(table).newAppend();
    }

    @Override
    public AppendFiles newFastAppend() {
      return txForTable(table).newFastAppend();
    }

    @Override
    public RewriteFiles newRewrite() {
      return txForTable(table).newRewrite();
    }

    @Override
    public RewriteManifests rewriteManifests() {
      return txForTable(table).rewriteManifests();
    }

    @Override
    public OverwriteFiles newOverwrite() {
      return txForTable(table).newOverwrite();
    }

    @Override
    public RowDelta newRowDelta() {
      return txForTable(table).newRowDelta();
    }

    @Override
    public ReplacePartitions newReplacePartitions() {
      return txForTable(table).newReplacePartitions();
    }

    @Override
    public DeleteFiles newDelete() {
      return txForTable(table).newDelete();
    }

    @Override
    public UpdateStatistics updateStatistics() {
      return txForTable(table).updateStatistics();
    }

    @Override
    public ExpireSnapshots expireSnapshots() {
      return txForTable(table).expireSnapshots();
    }

    @Override
    public ManageSnapshots manageSnapshots() {
      return txForTable(table).manageSnapshots();
    }
  }

  private class TransactionTableBuilder implements Catalog.TableBuilder {
    private final TableIdentifier identifier;
    private final Schema schema;
    private PartitionSpec spec = PartitionSpec.unpartitioned();
    private SortOrder sortOrder = SortOrder.unsorted();
    private String location = null;
    private final Map<String, String> tableProperties = Maps.newHashMap();

    private TransactionTableBuilder(TableIdentifier identifier, Schema schema) {
      this.identifier = identifier;
      this.schema = schema;
    }

    @Override
    public Catalog.TableBuilder withPartitionSpec(PartitionSpec newSpec) {
      this.spec = newSpec != null ? newSpec : PartitionSpec.unpartitioned();
      return this;
    }

    @Override
    public Catalog.TableBuilder withSortOrder(SortOrder newSortOrder) {
      this.sortOrder = newSortOrder != null ? newSortOrder : SortOrder.unsorted();
      return this;
    }

    @Override
    public Catalog.TableBuilder withLocation(String newLocation) {
      this.location = newLocation;
      return this;
    }

    @Override
    public Catalog.TableBuilder withProperties(Map<String, String> properties) {
      if (properties != null) {
        tableProperties.putAll(properties);
      }
      return this;
    }

    @Override
    public Catalog.TableBuilder withProperty(String key, String value) {
      tableProperties.put(key, value);
      return this;
    }

    @Override
    public Table create() {
      if (asCatalog().tableExists(identifier) || txByTable.containsKey(identifier)) {
        throw new AlreadyExistsException("Table already exists: %s", identifier);
      }
      Transaction txn =
          origin.newCreateTableTransaction(
              identifier, schema, spec, sortOrder, location, tableProperties);
      txByTable.put(identifier, txn);
      return txn.table();
    }

    @Override
    public Transaction createTransaction() {
      throw new UnsupportedOperationException(
          "CatalogTransaction tables do not support table level transaction");
    }

    @Override
    public Transaction replaceTransaction() {
      throw new UnsupportedOperationException(
          "CatalogTransaction tables do not support table level transaction");
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      throw new UnsupportedOperationException(
          "CatalogTransaction tables do not support table level transaction");
    }
  }

  public static class SerializationConflictException extends RuntimeException
      implements CleanableFailure {
    @FormatMethod
    public SerializationConflictException(String message, Object... args) {
      super(String.format(message, args));
    }

    @FormatMethod
    public SerializationConflictException(Throwable cause, String message, Object... args) {
      super(String.format(message, args), cause);
    }

    public SerializationConflictException(Throwable cause) {
      super(cause);
    }
  }
}
