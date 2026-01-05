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
package org.apache.iceberg.rest;

import static org.apache.iceberg.TestBase.FILE_A;
import static org.apache.iceberg.TestBase.FILE_A_DELETES;
import static org.apache.iceberg.TestBase.FILE_B;
import static org.apache.iceberg.TestBase.FILE_B_DELETES;
import static org.apache.iceberg.TestBase.SCHEMA;
import static org.apache.iceberg.TestBase.SPEC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end tests for ProduceSnapshotUpdateHandler.
 *
 * <p>These tests verify that the server-side snapshot production works correctly by going through
 * the full CatalogHandlers.commit() path.
 */
public class TestProduceSnapshotUpdateHandlerE2E {
  private static final Namespace NS = Namespace.of("ns");
  private static final TableIdentifier TABLE_ID = TableIdentifier.of(NS, "test_table");

  private InMemoryCatalog catalog;
  @TempDir private Path temp;

  @BeforeEach
  public void setUp() {
    File warehouse = temp.toFile();
    this.catalog = new InMemoryCatalog();
    this.catalog.initialize(
        "in-memory",
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.getAbsolutePath()));
    catalog.createNamespace(NS);
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (catalog != null) {
      catalog.close();
    }
  }

  private TableOperations getOps(Table table) {
    return ((BaseTable) table).operations();
  }

  /**
   * Creates a TableOperations wrapper that routes commits through CatalogHandlers.commit().
   * This simulates how a REST server processes commits with ProduceSnapshotUpdate support.
   */
  private TableOperations wrapOpsForProduceSnapshot(TableOperations delegate) {
    return new TableOperations() {
      @Override
      public TableMetadata current() {
        return delegate.current();
      }

      @Override
      public TableMetadata refresh() {
        return delegate.refresh();
      }

      @Override
      public void commit(TableMetadata base, TableMetadata metadata) {
        // Route through CatalogHandlers to process ProduceSnapshotUpdate
        List<MetadataUpdate> updates = metadata.changes();
        UpdateTableRequest request = new UpdateTableRequest(ImmutableList.of(), updates);
        CatalogHandlers.commit(delegate, request);
      }

      @Override
      public org.apache.iceberg.io.FileIO io() {
        return delegate.io();
      }

      @Override
      public String metadataFileLocation(String fileName) {
        return delegate.metadataFileLocation(fileName);
      }

      @Override
      public org.apache.iceberg.encryption.EncryptionManager encryption() {
        return delegate.encryption();
      }

      @Override
      public org.apache.iceberg.io.LocationProvider locationProvider() {
        return delegate.locationProvider();
      }
    };
  }

  @Test
  public void testAppendDataFiles() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();

    // Create a ProduceSnapshotUpdate for append
    MetadataUpdate.ProduceSnapshotUpdate produceUpdate =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.APPEND,
            ImmutableList.of(FILE_A),
            ImmutableList.of(), // no delete files
            ImmutableList.of(), // no removes
            ImmutableList.of(), // no delete file removes
            null, // no row filter
            false, // not staged
            SnapshotRef.MAIN_BRANCH,
            ImmutableMap.of("custom-key", "custom-value"),
            null, // no base snapshot ID
            null); // no validations

    // Commit through CatalogHandlers
    UpdateTableRequest request =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(produceUpdate));
    CatalogHandlers.commit(getOps(table), request);

    // Verify the snapshot was created
    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.APPEND);
    assertThat(snapshot.summary()).containsEntry("custom-key", "custom-value");
    assertThat(snapshot.summary()).containsEntry("added-data-files", "1");
    assertThat(snapshot.addedDataFiles(table.io()))
        .hasSize(1)
        .extracting(DataFile::location)
        .containsExactly(FILE_A.location().toString());
  }

  @Test
  public void testAppendMultipleDataFiles() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();

    // Append multiple files in one commit
    MetadataUpdate.ProduceSnapshotUpdate produceUpdate =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.APPEND,
            ImmutableList.of(FILE_A, FILE_B),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            false,
            SnapshotRef.MAIN_BRANCH,
            null,
            null,
            null);

    UpdateTableRequest request =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(produceUpdate));
    CatalogHandlers.commit(getOps(table), request);

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.summary()).containsEntry("added-data-files", "2");
    assertThat(snapshot.addedDataFiles(table.io())).hasSize(2);
  }

  @Test
  public void testDeleteDataFiles() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();

    // First, add a file using regular append
    table.newAppend().appendFile(FILE_A).commit();
    long parentSnapshotId = table.currentSnapshot().snapshotId();

    // Now delete using ProduceSnapshotUpdate
    DataFile fileToDelete =
        table.currentSnapshot().addedDataFiles(table.io()).iterator().next();

    MetadataUpdate.ProduceSnapshotUpdate produceUpdate =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.DELETE,
            ImmutableList.of(), // no adds
            ImmutableList.of(),
            ImmutableList.of(fileToDelete), // remove this file
            ImmutableList.of(),
            null,
            false,
            SnapshotRef.MAIN_BRANCH,
            null,
            parentSnapshotId,
            null);

    UpdateTableRequest request =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(produceUpdate));
    CatalogHandlers.commit(getOps(table), request);

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.DELETE);
    assertThat(snapshot.summary()).containsEntry("deleted-data-files", "1");
  }

  @Test
  public void testOverwriteDataFiles() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();

    // First, add a file
    table.newAppend().appendFile(FILE_A).commit();
    DataFile fileToDelete =
        table.currentSnapshot().addedDataFiles(table.io()).iterator().next();

    // Overwrite: remove FILE_A, add FILE_B
    MetadataUpdate.ProduceSnapshotUpdate produceUpdate =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.OVERWRITE,
            ImmutableList.of(FILE_B),
            ImmutableList.of(),
            ImmutableList.of(fileToDelete),
            ImmutableList.of(),
            null,
            false,
            SnapshotRef.MAIN_BRANCH,
            null,
            null,
            null);

    UpdateTableRequest request =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(produceUpdate));
    CatalogHandlers.commit(getOps(table), request);

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.OVERWRITE);
    assertThat(snapshot.summary()).containsEntry("added-data-files", "1");
    assertThat(snapshot.summary()).containsEntry("deleted-data-files", "1");
  }

  @Test
  public void testStageOnly() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();

    Snapshot originalSnapshot = table.currentSnapshot();

    // Stage a snapshot without making it current
    MetadataUpdate.ProduceSnapshotUpdate produceUpdate =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.APPEND,
            ImmutableList.of(FILE_A),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            true, // stageOnly
            SnapshotRef.MAIN_BRANCH,
            null,
            null,
            null);

    UpdateTableRequest request =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(produceUpdate));
    CatalogHandlers.commit(getOps(table), request);

    table.refresh();
    // Current snapshot should be unchanged (or still null if there was none)
    assertThat(table.currentSnapshot()).isEqualTo(originalSnapshot);

    // But there should be a new snapshot in the table's snapshot list
    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    if (originalSnapshot == null) {
      assertThat(snapshots).hasSize(1);
    } else {
      assertThat(snapshots).hasSize(2);
    }
  }

  @Test
  public void testValidationNoConflictingDataDetectsConflict() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();

    // Add FILE_A
    table.newAppend().appendFile(FILE_A).commit();
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    // Add FILE_B in a concurrent commit
    table.newAppend().appendFile(FILE_B).commit();

    // Now try to commit with validation that no conflicting data was added
    // The validation should fail because FILE_B was added after baseSnapshotId
    MetadataUpdate.ProduceSnapshotUpdate produceUpdate =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.OVERWRITE,
            ImmutableList.of(FILE_A),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            false,
            SnapshotRef.MAIN_BRANCH,
            null,
            baseSnapshotId,
            ImmutableList.of(new CommitValidation.NotAllowedAddedDataFiles(Expressions.alwaysTrue())));

    UpdateTableRequest request =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(produceUpdate));

    assertThatThrownBy(() -> CatalogHandlers.commit(getOps(table), request))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("conflicting files");
  }

  @Test
  public void testValidationNoConflictingDataWithFilter() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();

    table.newAppend().appendFile(FILE_A).commit();
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    // Add FILE_B which has a different partition value
    table.newAppend().appendFile(FILE_B).commit();

    // Validate only for partition where FILE_A lives (data = "0")
    // This should pass because FILE_B is in a different partition (data = "1")
    MetadataUpdate.ProduceSnapshotUpdate produceUpdate =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.OVERWRITE,
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            false,
            SnapshotRef.MAIN_BRANCH,
            null,
            baseSnapshotId,
            ImmutableList.of(
                new CommitValidation.NotAllowedAddedDataFiles(
                    Expressions.equal("data", "0"))));

    UpdateTableRequest request =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(produceUpdate));

    // This should succeed because the filter doesn't match the added file
    CatalogHandlers.commit(getOps(table), request);
    table.refresh();
    assertThat(table.currentSnapshot()).isNotNull();
  }

  @Test
  public void testSnapshotSummaryTotalsAccumulate() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();

    // First append
    MetadataUpdate.ProduceSnapshotUpdate append1 =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.APPEND,
            ImmutableList.of(FILE_A),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            false,
            SnapshotRef.MAIN_BRANCH,
            null,
            null,
            null);

    UpdateTableRequest request1 =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(append1));
    CatalogHandlers.commit(getOps(table), request1);

    table.refresh();
    Map<String, String> summary1 = table.currentSnapshot().summary();
    String totalFiles1 = summary1.get("total-data-files");

    // Second append
    MetadataUpdate.ProduceSnapshotUpdate append2 =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.APPEND,
            ImmutableList.of(FILE_B),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            false,
            SnapshotRef.MAIN_BRANCH,
            null,
            null,
            null);

    UpdateTableRequest request2 =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(append2));
    CatalogHandlers.commit(getOps(table), request2);

    table.refresh();
    Map<String, String> summary2 = table.currentSnapshot().summary();

    // Verify total-data-files accumulated
    if (totalFiles1 != null) {
      int expected = Integer.parseInt(totalFiles1) + 1;
      assertThat(summary2.get("total-data-files")).isEqualTo(String.valueOf(expected));
    }
  }

  @Test
  public void testSequenceNumbersAssigned() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();

    // First append
    MetadataUpdate.ProduceSnapshotUpdate append1 =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.APPEND,
            ImmutableList.of(FILE_A),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            false,
            SnapshotRef.MAIN_BRANCH,
            null,
            null,
            null);

    UpdateTableRequest request =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(append1));
    CatalogHandlers.commit(getOps(table), request);

    table.refresh();
    Snapshot snapshot1 = table.currentSnapshot();
    assertThat(snapshot1.sequenceNumber()).isGreaterThan(0);

    // Second append
    MetadataUpdate.ProduceSnapshotUpdate append2 =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.APPEND,
            ImmutableList.of(FILE_B),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            false,
            SnapshotRef.MAIN_BRANCH,
            null,
            null,
            null);

    UpdateTableRequest request2 =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(append2));
    CatalogHandlers.commit(getOps(table), request2);

    table.refresh();
    Snapshot snapshot2 = table.currentSnapshot();
    assertThat(snapshot2.sequenceNumber()).isGreaterThan(snapshot1.sequenceNumber());
  }

  @Test
  public void testParentSnapshotIdTracked() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();

    // First append
    table.newAppend().appendFile(FILE_A).commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();

    // Second append via ProduceSnapshotUpdate
    MetadataUpdate.ProduceSnapshotUpdate append =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.APPEND,
            ImmutableList.of(FILE_B),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            false,
            SnapshotRef.MAIN_BRANCH,
            null,
            null,
            null);

    UpdateTableRequest request =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(append));
    CatalogHandlers.commit(getOps(table), request);

    table.refresh();
    assertThat(table.currentSnapshot().parentId()).isEqualTo(firstSnapshotId);
  }

  @Test
  public void testInvalidActionRejected() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();

    MetadataUpdate.ProduceSnapshotUpdate produceUpdate =
        new MetadataUpdate.ProduceSnapshotUpdate(
            "invalid-action", // invalid
            ImmutableList.of(FILE_A),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            false,
            SnapshotRef.MAIN_BRANCH,
            null,
            null,
            null);

    UpdateTableRequest request =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(produceUpdate));

    assertThatThrownBy(() -> CatalogHandlers.commit(getOps(table), request))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported ProduceSnapshotUpdate action");
  }

  @Test
  public void testEmptyAppendCreatesSnapshot() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();

    // Empty append (no files)
    MetadataUpdate.ProduceSnapshotUpdate produceUpdate =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.APPEND,
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            false,
            SnapshotRef.MAIN_BRANCH,
            ImmutableMap.of("empty-commit", "true"),
            null,
            null);

    UpdateTableRequest request =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(produceUpdate));
    CatalogHandlers.commit(getOps(table), request);

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.APPEND);
    assertThat(snapshot.summary()).containsEntry("empty-commit", "true");
  }

  /**
   * Tests the client-side ProduceAppend class end-to-end.
   *
   * <p>This test verifies:
   * 1. ProduceAppend builds a ProduceSnapshotUpdate correctly
   * 2. The update is added to metadata changes
   * 3. When committed, the server creates the snapshot via CatalogHandlers
   */
  @Test
  public void testProduceAppendClientSideE2E() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();
    // Wrap ops to route through CatalogHandlers which processes ProduceSnapshotUpdate
    TableOperations ops = wrapOpsForProduceSnapshot(getOps(table));

    // Use the client-side ProduceAppend class
    ProduceAppend produceAppend = new ProduceAppend(TABLE_ID.toString(), ops);
    produceAppend.appendFile(FILE_A).appendFile(FILE_B);

    // Commit - this should:
    // 1. Build ProduceSnapshotUpdate
    // 2. Add it to metadata.changes()
    // 3. Call ops.commit() which routes through CatalogHandlers.commit()
    // 4. ProduceSnapshotUpdateHandler creates the snapshot server-side
    produceAppend.commit();

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.APPEND);
    assertThat(snapshot.addedDataFiles(table.io())).hasSize(2);
  }

  @Test
  public void testProduceDeleteClientSideE2E() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();

    // First add files with regular append
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.refresh();
    assertThat(table.currentSnapshot().addedDataFiles(table.io())).hasSize(2);

    // Wrap ops to route through CatalogHandlers
    TableOperations ops = wrapOpsForProduceSnapshot(getOps(table));

    // Now delete using ProduceDelete
    ProduceDelete produceDelete = new ProduceDelete(TABLE_ID.toString(), ops);
    produceDelete.deleteFile(FILE_A);
    produceDelete.commit();

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.DELETE);
    assertThat(snapshot.removedDataFiles(table.io())).hasSize(1);
  }

  @Test
  public void testProduceOverwriteClientSideE2E() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();

    // First add FILE_A
    table.newAppend().appendFile(FILE_A).commit();
    table.refresh();

    // Wrap ops to route through CatalogHandlers
    TableOperations ops = wrapOpsForProduceSnapshot(getOps(table));

    // Use ProduceOverwrite to replace FILE_A with FILE_B
    ProduceOverwrite produceOverwrite = new ProduceOverwrite(TABLE_ID.toString(), ops);
    produceOverwrite.deleteFile(FILE_A).addFile(FILE_B);
    produceOverwrite.commit();

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.OVERWRITE);
    assertThat(snapshot.addedDataFiles(table.io())).hasSize(1);
    assertThat(snapshot.removedDataFiles(table.io())).hasSize(1);
  }

  @Test
  public void testProduceAppendToBranch() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();

    // Create a branch first using normal operations
    table.manageSnapshots().createBranch("test-branch").commit();
    table.refresh();

    // Wrap ops to route through CatalogHandlers
    TableOperations ops = wrapOpsForProduceSnapshot(getOps(table));

    // Use ProduceAppend to append to the branch
    ProduceAppend produceAppend = new ProduceAppend(TABLE_ID.toString(), ops);
    produceAppend.appendFile(FILE_A).toBranch("test-branch");
    produceAppend.commit();

    table.refresh();
    Snapshot branchSnapshot = table.snapshot("test-branch");
    assertThat(branchSnapshot).isNotNull();
    assertThat(branchSnapshot.addedDataFiles(table.io())).hasSize(1);

    // Main branch should be unchanged
    assertThat(table.currentSnapshot()).isNull();
  }

  @Test
  public void testProduceAppendWithSummaryProperties() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();
    // Wrap ops to route through CatalogHandlers
    TableOperations ops = wrapOpsForProduceSnapshot(getOps(table));

    ProduceAppend produceAppend = new ProduceAppend(TABLE_ID.toString(), ops);
    produceAppend.appendFile(FILE_A).set("custom-property", "custom-value");
    produceAppend.commit();

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.summary()).containsEntry("custom-property", "custom-value");
  }

  @Test
  public void testValidationRequiredDataFilesDetectsMissingFiles() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();

    // Add FILE_A
    table.newAppend().appendFile(FILE_A).commit();
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    // Concurrent commit deletes FILE_A
    DataFile fileToDelete = table.currentSnapshot().addedDataFiles(table.io()).iterator().next();
    table.newDelete().deleteFile(fileToDelete).commit();

    // Now try to commit requiring FILE_A to still exist
    MetadataUpdate.ProduceSnapshotUpdate produceUpdate =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.APPEND,
            ImmutableList.of(FILE_B),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            false,
            SnapshotRef.MAIN_BRANCH,
            null,
            baseSnapshotId,
            ImmutableList.of(
                new CommitValidation.RequiredDataFiles(
                    Expressions.alwaysTrue(),
                    ImmutableList.of(fileToDelete.location().toString()))));

    UpdateTableRequest request =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(produceUpdate));

    assertThatThrownBy(() -> CatalogHandlers.commit(getOps(table), request))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("missing data file");
  }

  @Test
  public void testValidationRequiredDeleteFilesDetectsMissingFiles() {
    // Create V2 table to support delete files
    Table table =
        catalog
            .buildTable(TABLE_ID, SCHEMA)
            .withPartitionSpec(SPEC)
            .withProperty(TableProperties.FORMAT_VERSION, "2")
            .create();

    // Add data file and then a positional delete file
    table.newAppend().appendFile(FILE_A).commit();
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    // Get the actual delete file from the snapshot
    DeleteFile deleteFile =
        table.currentSnapshot().addedDeleteFiles(table.io()).iterator().next();

    // Concurrent commit removes the delete file via rewrite (replacing it with nothing)
    table.newRewrite().deleteFile(deleteFile).commit();

    // Now try to commit requiring that delete file to still exist
    MetadataUpdate.ProduceSnapshotUpdate produceUpdate =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.APPEND,
            ImmutableList.of(FILE_B),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            false,
            SnapshotRef.MAIN_BRANCH,
            null,
            baseSnapshotId,
            ImmutableList.of(
                new CommitValidation.RequiredDeleteFiles(
                    Expressions.alwaysTrue(),
                    ImmutableList.of(deleteFile.location().toString()))));

    UpdateTableRequest request =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(produceUpdate));

    assertThatThrownBy(() -> CatalogHandlers.commit(getOps(table), request))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("missing delete file");
  }

  @Test
  public void testValidationNotAllowedAddedDeleteFilesDetectsConflict() {
    // Create V2 table to support delete files
    Table table =
        catalog
            .buildTable(TABLE_ID, SCHEMA)
            .withPartitionSpec(SPEC)
            .withProperty(TableProperties.FORMAT_VERSION, "2")
            .create();

    // Add data file
    table.newAppend().appendFile(FILE_A).commit();
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    // Concurrent commit adds a delete file
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();

    // Now try to commit with validation that no delete files were added
    MetadataUpdate.ProduceSnapshotUpdate produceUpdate =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.OVERWRITE,
            ImmutableList.of(FILE_B),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            false,
            SnapshotRef.MAIN_BRANCH,
            null,
            baseSnapshotId,
            ImmutableList.of(new CommitValidation.NotAllowedAddedDeleteFiles(Expressions.alwaysTrue())));

    UpdateTableRequest request =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(produceUpdate));

    assertThatThrownBy(() -> CatalogHandlers.commit(getOps(table), request))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("conflicting delete files");
  }

  @Test
  public void testAddDeleteFilesInProduceSnapshot() {
    // Create V2 table to support delete files
    Table table =
        catalog
            .buildTable(TABLE_ID, SCHEMA)
            .withPartitionSpec(SPEC)
            .withProperty(TableProperties.FORMAT_VERSION, "2")
            .create();

    // Add data file first
    table.newAppend().appendFile(FILE_A).commit();

    // Use ProduceSnapshotUpdate to add delete files
    MetadataUpdate.ProduceSnapshotUpdate produceUpdate =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.OVERWRITE,
            ImmutableList.of(), // no data files
            ImmutableList.of(FILE_A_DELETES), // add delete file
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            false,
            SnapshotRef.MAIN_BRANCH,
            null,
            null,
            null);

    UpdateTableRequest request =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(produceUpdate));
    CatalogHandlers.commit(getOps(table), request);

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.OVERWRITE);
    assertThat(snapshot.addedDeleteFiles(table.io()))
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A_DELETES.location().toString());
  }

  @Test
  public void testRemoveDeleteFilesInProduceSnapshot() {
    // Create V2 table to support delete files
    Table table =
        catalog
            .buildTable(TABLE_ID, SCHEMA)
            .withPartitionSpec(SPEC)
            .withProperty(TableProperties.FORMAT_VERSION, "2")
            .create();

    // Add data file and delete file
    table.newAppend().appendFile(FILE_A).commit();
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();

    DeleteFile deleteFile =
        table.currentSnapshot().addedDeleteFiles(table.io()).iterator().next();

    // Use ProduceSnapshotUpdate to remove the delete file
    MetadataUpdate.ProduceSnapshotUpdate produceUpdate =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.REPLACE,
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(), // no data files removed
            ImmutableList.of(deleteFile), // remove delete file
            null,
            false,
            SnapshotRef.MAIN_BRANCH,
            null,
            null,
            null);

    UpdateTableRequest request =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(produceUpdate));
    CatalogHandlers.commit(getOps(table), request);

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.REPLACE);
    assertThat(snapshot.removedDeleteFiles(table.io()))
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(deleteFile.location().toString());
  }

  @Test
  public void testMultipleValidationsAllApplied() {
    Table table = catalog.buildTable(TABLE_ID, SCHEMA).withPartitionSpec(SPEC).create();

    // Add FILE_A
    table.newAppend().appendFile(FILE_A).commit();
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    // Concurrent commit adds FILE_B
    table.newAppend().appendFile(FILE_B).commit();

    // Try to commit with multiple validations - the NotAllowedAddedDataFiles should fail
    MetadataUpdate.ProduceSnapshotUpdate produceUpdate =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.OVERWRITE,
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            false,
            SnapshotRef.MAIN_BRANCH,
            null,
            baseSnapshotId,
            ImmutableList.of(
                // This should pass - just a filter with no specific file paths
                new CommitValidation.RequiredDataFiles(Expressions.alwaysFalse(), ImmutableList.of()),
                // This should fail - FILE_B was added
                new CommitValidation.NotAllowedAddedDataFiles(Expressions.alwaysTrue())));

    UpdateTableRequest request =
        new UpdateTableRequest(ImmutableList.of(), ImmutableList.of(produceUpdate));

    assertThatThrownBy(() -> CatalogHandlers.commit(getOps(table), request))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("conflicting files");
  }
}
