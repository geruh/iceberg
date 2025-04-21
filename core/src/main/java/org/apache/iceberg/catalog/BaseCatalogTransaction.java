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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.DataTableScan;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.UpdateStatistics;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalog;
import org.immutables.value.Value;

public class BaseCatalogTransaction implements CatalogTransaction {
  private final Catalog catalog;
  private final IsolationLevel isolationLevel;
  private final String transactionId;
  private final Map<TableIdentifier, Transaction> tableTransactions;
  private boolean hasCommitted = false;

  public BaseCatalogTransaction(
      Catalog catalog, IsolationLevel isolationLevel, String transactionId) {
    Preconditions.checkArgument(null != catalog, "Invalid origin catalog: null");
    Preconditions.checkArgument(null != transactionId, "Invalid origin catalog: null");
    Preconditions.checkArgument(null != isolationLevel, "Invalid isolation level: null");
    Preconditions.checkArgument(
        catalog instanceof SupportsCatalogTransactions,
        "Origin catalog does not support catalog transactions");
    this.catalog = catalog;
    this.isolationLevel = isolationLevel;
    this.transactionId = transactionId;
    this.tableTransactions = Maps.newHashMap();
  }

  @Override
  public String id() {
    return transactionId;
  }

  @Override
  public IsolationLevel isolationLevel() {
    return isolationLevel;
  }

  @Override
  public Catalog asCatalog() {
    return new AsTransactionalCatalog();
  }

  @Override
  public void commitTransaction() {
    Preconditions.checkState(!hasCommitted, "Transaction has already committed changes");

    try {
      List<TableCommit> tableCommits =
          tableTransactions.entrySet().stream()
              .filter(e -> e.getValue() instanceof BaseTransaction)
              .map(
                  e ->
                      TableCommit.create(
                          e.getKey(),
                          ((BaseTransaction) e.getValue()).startMetadata(),
                          ((BaseTransaction) e.getValue()).currentMetadata()))
              .collect(Collectors.toList());
      if (!tableCommits.isEmpty()) {
        ((RESTCatalog) catalog).commitTransaction(tableCommits);
      }
      hasCommitted = true;
    } catch (CommitStateUnknownException e) {
      throw e;
    } catch (RuntimeException e) {
      rollback();
      throw e;
    }
  }

  //  private boolean hasUpdates() {
  //    return tableTransactions.values().stream()
  //        .filter(tx -> tx instanceof BaseTransaction)
  //        .map(tx -> (BaseTransaction) tx)
  //        .anyMatch(tx -> !tx.currentMetadata().changes().isEmpty());
  //  }

  //  /**
  //   * With SERIALIZABLE isolation we mainly need to check that write skew is not possible. Write
  // skew
  //   * happens due to a transaction taking action based on an outdated premise (a fact that was
  // true
  //   * when a table was initially loaded but then changed due to a concurrent update to the table
  //   * while this TX was in-progress). When this TX wants to commit, the original premise might
  // not
  //   * hold anymore, thus we need to check whether the {@link org.apache.iceberg.Snapshot} a
  // branch
  //   * was pointing to changed after it was initially read inside this TX. If no information of a
  //   * branch's snapshot is available, we check whether {@link TableMetadata} changed after it was
  //   * initially read.
  //   */
  //  private void validateSerializableIsolation() {
  //    for (TableRef readTable : initiallyReadTableMetadataByRef.keySet()) {
  //      // check all read tables to determine whether they changed outside the catalog
  //      // TX after they were initially read on a particular branch
  //      if (IsolationLevel.SERIALIZABLE == isolationLevel) {
  //        BaseTable table = (BaseTable) origin.loadTable(readTable.identifier());
  //        SnapshotRef snapshotRef = table.operations().current().ref(readTable.ref());
  //        SnapshotRef snapshotRefInsideTx =
  //            initiallyReadTableMetadataByRef.get(readTable).ref(readTable.ref());
  //
  //        if (null != snapshotRef
  //            && null != snapshotRefInsideTx
  //            && snapshotRef.snapshotId() != snapshotRefInsideTx.snapshotId()) {
  //          throw new ValidationException(
  //              "%s isolation violation: Found table metadata updates to table '%s' after it was
  // read on branch '%s'",
  //              isolationLevel(), readTable.identifier().toString(), readTable.ref());
  //        }
  //
  //        if (null == snapshotRef || null == snapshotRefInsideTx) {
  //          TableMetadata currentTableMetadata = table.operations().current();
  //
  //          if (!currentTableMetadata
  //              .metadataFileLocation()
  //              .equals(initiallyReadTableMetadataByRef.get(readTable).metadataFileLocation())) {
  //            throw new ValidationException(
  //                "%s isolation violation: Found table metadata updates to table '%s' after it was
  // read",
  //                isolationLevel(), readTable.identifier());
  //          }
  //        }
  //      }
  //    }
  //  }

  private void rollback() {
    if (!hasCommitted) {
      //      catalog.abortTransaction(id());
      tableTransactions.clear();
    }
  }

  private Optional<Table> txTable(TableIdentifier identifier) {
    if (tableTransactions.containsKey(identifier)) {
      return Optional.ofNullable(tableTransactions.get(identifier).table());
    }
    return Optional.empty();
  }

  /**
   * We're using a {@link Transaction} per table so that we can keep track of pending changes for a
   * particular table.
   */
  private Transaction txForTable(Table table) {
    TableMetadata tableMetadata = ((HasTableOperations) table).operations().current();
    return tableTransactions.computeIfAbsent(
        identifierWithoutCatalog(table.name()),
        k ->
            Transactions.newTransaction(
                table.name(), ((HasTableOperations) table).operations(), tableMetadata));
  }

  // TODO: this functionality should probably live somewhere else to be reusable
  private TableIdentifier identifierWithoutCatalog(String tableWithCatalog) {
    if (tableWithCatalog.startsWith(catalog.name())) {
      return TableIdentifier.parse(tableWithCatalog.replace(catalog.name() + ".", ""));
    }
    return TableIdentifier.parse(tableWithCatalog);
  }

  private class AsTransactionalCatalog implements Catalog {
    @Override
    public Table loadTable(TableIdentifier identifier) {
      Transaction existing = tableTransactions.get(identifier);
      if (existing != null) {
        return existing.table();
      }

      Table table =
          BaseCatalogTransaction.this
              .txTable(identifier)
              .orElseGet(
                  () ->
                      ((SupportsCatalogTransactions) catalog)
                          .loadTableInTransaction(identifier, transactionId));

      return new TransactionalTable(table, opsFromTable(table));
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return catalog.listTables(namespace);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      return catalog.dropTable(identifier, purge);
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
      catalog.renameTable(from, to);
    }
  }

  private static TableOperations opsFromTable(Table table) {
    return table instanceof BaseTransaction.TransactionTable
        ? ((BaseTransaction.TransactionTable) table).operations()
        : ((BaseTable) table).operations();
  }

  private class TransactionalTable extends BaseTable {
    private final Table table;

    private TransactionalTable(Table table, TableOperations ops) {
      super(ops, table.name());
      this.table = table;
    }

    @Override
    public TableScan newScan() {
      TableScan tableScan = super.newScan();
      if (tableScan instanceof DataTableScan) {
        return new TransactionalTableScan((DataTableScan) tableScan);
      }
      return tableScan;
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

  private static class TransactionalTableScan extends DataTableScan {
    protected TransactionalTableScan(DataTableScan delegate) {
      super(delegate.table(), delegate.schema(), delegate.context());
    }

    @Override
    public TableScan useRef(String name) {
      return (DataTableScan) super.useRef(name);
    }
  }

  @Value.Immutable
  interface TableRef {
    TableIdentifier identifier();

    String ref();

    @Value.Lazy
    default String name() {
      return identifier().toString() + "@" + ref();
    }
  }
}
