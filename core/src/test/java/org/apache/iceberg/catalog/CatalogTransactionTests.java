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

import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public abstract class CatalogTransactionTests<
        C extends Catalog & SupportsNamespaces & SupportsCatalogTransactions>
    extends CatalogTests<C> {

  @Test
  public void testBasicTransaction() {
    C catalog = catalog();
    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }
    CatalogTransaction tx = catalog.beginTransaction(IsolationLevel.SNAPSHOT);

    Table table = tx.asCatalog().buildTable(TABLE, SCHEMA).create();
    table.updateSchema().addColumn("comment", Types.StringType.get()).commit();

    tx.commitTransaction();
    Table committedTable = catalog.loadTable(TABLE);
    Assertions.assertThat(committedTable.schema().findField("comment")).isNotNull();
  }

  @Test
  public void testMultiTableTransaction() {
    C catalog = catalog();
    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    TableIdentifier table1 = TableIdentifier.of(NS, "table1");
    TableIdentifier table2 = TableIdentifier.of(NS, "table2");

    CatalogTransaction tx = catalog.beginTransaction(IsolationLevel.SNAPSHOT);

    Table t1 = tx.asCatalog().buildTable(table1, SCHEMA).create();
    Table t2 = tx.asCatalog().buildTable(table2, SCHEMA).create();

    t1.updateSchema().addColumn("col1", Types.StringType.get()).commit();
    t2.updateSchema().addColumn("col2", Types.StringType.get()).commit();

    tx.commitTransaction();

    Assertions.assertThat(catalog.loadTable(table1).schema().findField("col1")).isNotNull();
    Assertions.assertThat(catalog.loadTable(table2).schema().findField("col2")).isNotNull();
  }

  @Test
  public void testSerializableIsolation() {
    C catalog = catalog();

    Assertions.assertThat(catalog.tableExists(TABLE)).as("Table should not exist").isFalse();
    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    catalog.buildTable(TABLE, SCHEMA).create();
    Assertions.assertThat(catalog.tableExists(TABLE)).as("Table should exist").isTrue();

    CatalogTransaction tx1 = catalog.beginTransaction(IsolationLevel.SERIALIZABLE);
    CatalogTransaction tx2 = catalog.beginTransaction(IsolationLevel.SERIALIZABLE);

    // Load table in both transactions
    Table table1 = tx1.asCatalog().loadTable(TABLE);
    Table table2 = tx2.asCatalog().loadTable(TABLE);

    // Modify in first transaction
    table1.updateSchema().addColumn("col1", Types.StringType.get()).commit();
    tx1.commitTransaction();

    Assertions.assertThatThrownBy(
            () -> {
              table2.updateSchema().addColumn("col2", Types.StringType.get()).commit();
              tx2.commitTransaction();
            })
        .isInstanceOf(BaseCatalogTransaction.SerializationConflictException.class);
  }

  @Test
  public void testSnapshotIsolationRebase() {
    C catalog = catalog();
    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).create();
    Assertions.assertThat(catalog.tableExists(TABLE)).as("Table should exist").isTrue();
    CatalogTransaction txn = catalog.beginTransaction(IsolationLevel.SNAPSHOT);
    Catalog txnCatalog = txn.asCatalog();

    Table table2 = txnCatalog.loadTable(TABLE);

    // add file to table outside of transaction
    table.newFastAppend().appendFile(FILE_B).commit();

    // transaction shouldn't see changes
    assertFiles(table2);

    // add a new file to transaction
    table2.newFastAppend().appendFile(FILE_A).commit();

    txn.commitTransaction();

    // should have rebased changes on top of latest
    assertFiles(table2, FILE_A, FILE_B);
  }

  @Test
  public void testDropTableInConcurrentTransactionFails() {
    C catalog = catalog();
    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    catalog.buildTable(TABLE, SCHEMA).create();
    Assertions.assertThat(catalog.tableExists(TABLE)).as("Table should exist").isTrue();

    CatalogTransaction txn = catalog.beginTransaction(IsolationLevel.SNAPSHOT);

    Table table2 = txn.asCatalog().loadTable(TABLE);
    table2.newFastAppend().appendFile(FILE_A).commit();

    // drop table outside of transaction
    catalog.dropTable(TABLE);

    Assertions.assertThatThrownBy(txn::commitTransaction).isInstanceOf(NoSuchTableException.class);
  }

  @Test
  public void testConcurrentTableCreationFails() {
    C catalog = catalog();
    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    CatalogTransaction tx1 = catalog.beginTransaction(IsolationLevel.SERIALIZABLE);
    CatalogTransaction tx2 = catalog.beginTransaction(IsolationLevel.SERIALIZABLE);

    // Create table in first transaction
    tx1.asCatalog().buildTable(TABLE, SCHEMA).create();
    tx1.commitTransaction();

    Assertions.assertThatThrownBy(() -> tx2.asCatalog().buildTable(TABLE, SCHEMA).create())
        .isInstanceOf(AlreadyExistsException.class);
  }
}
