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
import static org.apache.iceberg.TestBase.FILE_B;
import static org.apache.iceberg.TestBase.FILE_C;
import static org.apache.iceberg.TestBase.SCHEMA;
import static org.apache.iceberg.TestBase.SPEC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * End-to-end REST catalog tests for ProduceSnapshotUpdate (server-side snapshot creation).
 *
 * <p>These tests verify that ProduceAppend, ProduceDelete, and ProduceOverwrite work correctly
 * through the full REST API stack, similar to how TestRESTScanPlanning tests scan planning.
 */
public class TestRESTProduceSnapshot {
  private static final ObjectMapper MAPPER = RESTObjectMapper.mapper();
  private static final Namespace NS = Namespace.of("ns");

  private InMemoryCatalog backendCatalog;
  private Server httpServer;
  private RESTCatalogAdapter adapterForRESTServer;
  private ParserContext parserContext;
  @TempDir private Path temp;
  private RESTCatalog restCatalog;

  @BeforeEach
  public void setupCatalogs() throws Exception {
    File warehouse = temp.toFile();
    this.backendCatalog = new InMemoryCatalog();
    this.backendCatalog.initialize(
        "in-memory",
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.getAbsolutePath()));

    adapterForRESTServer =
        Mockito.spy(
            new RESTCatalogAdapter(backendCatalog) {
              @Override
              public <T extends RESTResponse> T execute(
                  HTTPRequest request,
                  Class<T> responseType,
                  Consumer<ErrorResponse> errorHandler,
                  Consumer<Map<String, String>> responseHeaders) {
                if (ResourcePaths.config().equals(request.path())) {
                  return castResponse(
                      responseType,
                      ConfigResponse.builder()
                          .withEndpoints(
                              Arrays.stream(Route.values())
                                  .map(r -> Endpoint.create(r.method().name(), r.resourcePath()))
                                  .collect(Collectors.toList()))
                          .withOverrides(
                              ImmutableMap.of(
                                  RESTCatalogProperties.REST_SERVER_SIDE_COMMITS_ENABLED, "true"))
                          .build());
                }
                Object body = roundTripSerialize(request.body(), "request");
                HTTPRequest req = ImmutableHTTPRequest.builder().from(request).body(body).build();
                T response = super.execute(req, responseType, errorHandler, responseHeaders);
                return roundTripSerialize(response, "response");
              }
            });

    ServletContextHandler servletContext =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    servletContext.addServlet(
        new ServletHolder(new RESTCatalogServlet(adapterForRESTServer)), "/*");
    servletContext.setHandler(new GzipHandler());

    this.httpServer = new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    httpServer.setHandler(servletContext);
    httpServer.start();

    // Initialize catalog with server-side commits enabled
    this.restCatalog = initCatalog("prod-with-server-commits", ImmutableMap.of());
  }

  @AfterEach
  public void teardownCatalogs() throws Exception {
    if (restCatalog != null) {
      restCatalog.close();
    }

    if (backendCatalog != null) {
      backendCatalog.close();
    }

    if (httpServer != null) {
      httpServer.stop();
      httpServer.join();
    }
  }

  // ==================== Helper Methods ====================

  private RESTCatalog initCatalog(String catalogName, Map<String, String> additionalProperties) {
    RESTCatalog catalog =
        new RESTCatalog(
            SessionCatalog.SessionContext.createEmpty(),
            (config) ->
                HTTPClient.builder(config)
                    .uri(config.get(CatalogProperties.URI))
                    .withHeaders(RESTUtil.configHeaders(config))
                    .build());
    catalog.setConf(new Configuration());
    Map<String, String> properties =
        ImmutableMap.of(
            CatalogProperties.URI,
            httpServer.getURI().toString(),
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO");
    catalog.initialize(
        catalogName,
        ImmutableMap.<String, String>builder()
            .putAll(properties)
            .putAll(additionalProperties)
            .build());
    return catalog;
  }

  @SuppressWarnings("unchecked")
  private <T> T roundTripSerialize(T payload, String description) {
    if (payload == null) {
      return null;
    }

    try {
      if (payload instanceof RESTMessage) {
        RESTMessage message = (RESTMessage) payload;
        ObjectReader reader = MAPPER.readerFor(message.getClass());
        if (parserContext != null && !parserContext.isEmpty()) {
          reader = reader.with(parserContext.toInjectableValues());
        }
        return reader.readValue(MAPPER.writeValueAsString(message));
      } else {
        // use Map so that Jackson doesn't try to instantiate ImmutableMap from payload.getClass()
        return (T) MAPPER.readValue(MAPPER.writeValueAsString(payload), Map.class);
      }
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          String.format("Failed to serialize and deserialize %s: %s", description, payload), e);
    }
  }

  private void setParserContext(Table table) {
    parserContext =
        ParserContext.builder().add("specsById", table.specs()).add("caseSensitive", false).build();
  }

  private Table createTable(String tableName) {
    return createTable(TableIdentifier.of(NS, tableName));
  }

  private Table createTable(TableIdentifier identifier) {
    restCatalog.createNamespace(identifier.namespace());
    return restCatalog.buildTable(identifier, SCHEMA).withPartitionSpec(SPEC).create();
  }

  private RESTTable asRESTTable(Table table) {
    assertThat(table).isInstanceOf(RESTTable.class);
    return (RESTTable) table;
  }

  // ==================== ProduceAppend Tests ====================

  @Test
  public void testProduceAppendSingleFile() {
    Table table = createTable("append_single_file");
    setParserContext(table);

    // Use server-side commit via RESTTable
    RESTTable restTable = asRESTTable(table);
    restTable.newAppend().appendFile(FILE_A).commit();

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.APPEND);
    assertThat(snapshot.addedDataFiles(table.io())).hasSize(1);
    assertThat(snapshot.addedDataFiles(table.io()))
        .first()
        .extracting(DataFile::location)
        .isEqualTo(FILE_A.location());
  }

  @Test
  public void testProduceAppendMultipleFiles() {
    Table table = createTable("append_multiple_files");
    setParserContext(table);

    RESTTable restTable = asRESTTable(table);
    restTable.newAppend().appendFile(FILE_A).appendFile(FILE_B).appendFile(FILE_C).commit();

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.APPEND);
    assertThat(snapshot.addedDataFiles(table.io())).hasSize(3);
  }

  @Test
  public void testProduceAppendMultipleCommits() {
    Table table = createTable("append_multiple_commits");
    setParserContext(table);

    RESTTable restTable = asRESTTable(table);

    // First append
    restTable.newAppend().appendFile(FILE_A).commit();
    table.refresh();
    long snapshot1Id = table.currentSnapshot().snapshotId();

    // Second append
    restTable.newAppend().appendFile(FILE_B).commit();
    table.refresh();
    long snapshot2Id = table.currentSnapshot().snapshotId();

    assertThat(snapshot2Id).isNotEqualTo(snapshot1Id);
    assertThat(table.currentSnapshot().parentId()).isEqualTo(snapshot1Id);
    assertThat(table.snapshots()).hasSize(2);
  }

  @Test
  public void testProduceAppendToBranch() {
    Table table = createTable("append_to_branch");
    setParserContext(table);

    // Create a branch
    table.manageSnapshots().createBranch("test-branch").commit();
    table.refresh();

    RESTTable restTable = asRESTTable(table);
    restTable.newAppend().appendFile(FILE_A).toBranch("test-branch").commit();

    table.refresh();
    Snapshot branchSnapshot = table.snapshot("test-branch");
    assertThat(branchSnapshot).isNotNull();
    assertThat(branchSnapshot.addedDataFiles(table.io())).hasSize(1);

    // Main branch should be unchanged (null since no commits to main)
    assertThat(table.currentSnapshot()).isNull();
  }

  @Test
  public void testProduceAppendWithSummaryProperties() {
    Table table = createTable("append_with_summary");
    setParserContext(table);

    RESTTable restTable = asRESTTable(table);
    restTable
        .newAppend()
        .appendFile(FILE_A)
        .set("custom-property", "custom-value")
        .set("commit-id", "abc123")
        .commit();

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.summary()).containsEntry("custom-property", "custom-value");
    assertThat(snapshot.summary()).containsEntry("commit-id", "abc123");
  }

  @Test
  public void testProduceAppendEmptyCommit() {
    Table table = createTable("append_empty");
    setParserContext(table);

    RESTTable restTable = asRESTTable(table);
    // Empty append - should still create a snapshot
    restTable.newAppend().set("empty-commit", "true").commit();

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.APPEND);
    assertThat(snapshot.summary()).containsEntry("empty-commit", "true");
    assertThat(snapshot.addedDataFiles(table.io())).isEmpty();
  }

  @Test
  public void testProduceAppendManifestNotSupported() {
    Table table = createTable("append_manifest_unsupported");
    RESTTable restTable = asRESTTable(table);

    assertThatThrownBy(() -> restTable.newAppend().appendManifest(null))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("ProduceAppend does not support appendManifest");
  }

  // ==================== ProduceDelete Tests ====================

  @Test
  public void testProduceDeleteSingleFile() {
    Table table = createTable("delete_single_file");
    setParserContext(table);

    // First add files using regular append
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.refresh();

    // Now delete using server-side commit
    RESTTable restTable = asRESTTable(table);
    restTable.newDelete().deleteFile(FILE_A).commit();

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.DELETE);
    assertThat(snapshot.removedDataFiles(table.io())).hasSize(1);
    assertThat(snapshot.removedDataFiles(table.io()))
        .first()
        .extracting(DataFile::location)
        .isEqualTo(FILE_A.location());
  }

  @Test
  public void testProduceDeleteMultipleFiles() {
    Table table = createTable("delete_multiple_files");
    setParserContext(table);

    // First add files
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).appendFile(FILE_C).commit();
    table.refresh();

    // Delete multiple files
    RESTTable restTable = asRESTTable(table);
    restTable.newDelete().deleteFile(FILE_A).deleteFile(FILE_B).commit();

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.DELETE);
    assertThat(snapshot.removedDataFiles(table.io())).hasSize(2);
  }

  @Test
  public void testProduceDeleteToBranch() {
    Table table = createTable("delete_to_branch");
    setParserContext(table);

    // Add files to main
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.refresh();
    long mainSnapshot = table.currentSnapshot().snapshotId();

    // Create a branch from current snapshot
    table.manageSnapshots().createBranch("delete-branch", mainSnapshot).commit();
    table.refresh();

    // Delete from branch
    RESTTable restTable = asRESTTable(table);
    restTable.newDelete().deleteFile(FILE_A).toBranch("delete-branch").commit();

    table.refresh();
    Snapshot branchSnapshot = table.snapshot("delete-branch");
    assertThat(branchSnapshot.operation()).isEqualTo(DataOperations.DELETE);
    assertThat(branchSnapshot.removedDataFiles(table.io())).hasSize(1);

    // Main branch should be unchanged
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(mainSnapshot);
  }

  // ==================== ProduceOverwrite Tests ====================

  @Test
  public void testProduceOverwriteAddAndDelete() {
    Table table = createTable("overwrite_add_delete");
    setParserContext(table);

    // First add FILE_A
    table.newAppend().appendFile(FILE_A).commit();
    table.refresh();

    // Overwrite: remove FILE_A, add FILE_B
    RESTTable restTable = asRESTTable(table);
    restTable.newOverwrite().deleteFile(FILE_A).addFile(FILE_B).commit();

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.OVERWRITE);
    assertThat(snapshot.addedDataFiles(table.io())).hasSize(1);
    assertThat(snapshot.removedDataFiles(table.io())).hasSize(1);
    assertThat(snapshot.addedDataFiles(table.io()))
        .first()
        .extracting(DataFile::location)
        .isEqualTo(FILE_B.location());
    assertThat(snapshot.removedDataFiles(table.io()))
        .first()
        .extracting(DataFile::location)
        .isEqualTo(FILE_A.location());
  }

  @Test
  public void testProduceOverwriteMultipleFiles() {
    Table table = createTable("overwrite_multiple");
    setParserContext(table);

    // Add initial files
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.refresh();

    // Overwrite: remove FILE_A and FILE_B, add FILE_C
    RESTTable restTable = asRESTTable(table);
    restTable.newOverwrite().deleteFile(FILE_A).deleteFile(FILE_B).addFile(FILE_C).commit();

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.OVERWRITE);
    assertThat(snapshot.addedDataFiles(table.io())).hasSize(1);
    assertThat(snapshot.removedDataFiles(table.io())).hasSize(2);
  }

  @Test
  public void testProduceOverwriteToBranch() {
    Table table = createTable("overwrite_to_branch");
    setParserContext(table);

    // Add FILE_A to main
    table.newAppend().appendFile(FILE_A).commit();
    table.refresh();
    long mainSnapshot = table.currentSnapshot().snapshotId();

    // Create branch
    table.manageSnapshots().createBranch("overwrite-branch", mainSnapshot).commit();
    table.refresh();

    // Overwrite on branch
    RESTTable restTable = asRESTTable(table);
    restTable
        .newOverwrite()
        .deleteFile(FILE_A)
        .addFile(FILE_B)
        .toBranch("overwrite-branch")
        .commit();

    table.refresh();
    Snapshot branchSnapshot = table.snapshot("overwrite-branch");
    assertThat(branchSnapshot.operation()).isEqualTo(DataOperations.OVERWRITE);

    // Main should be unchanged
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(mainSnapshot);
  }

  // ==================== Multiple Partition Specs Tests ====================

  @Test
  public void testProduceAppendWithMultiplePartitionSpecs() {
    Table table = createTable("multiple_specs");
    setParserContext(table);

    // Add files with original spec
    RESTTable restTable = asRESTTable(table);
    restTable.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.refresh();

    // Evolve partition spec
    table.updateSpec().removeField("data_bucket").addField(org.apache.iceberg.expressions.Expressions.bucket("data", 8)).commit();
    table.refresh();

    PartitionSpec newSpec = table.spec();
    assertThat(newSpec.specId()).isEqualTo(1);

    // Create data file with new partition spec
    DataFile fileWithNewSpec =
        DataFiles.builder(newSpec)
            .withPath("/path/to/data-new-spec.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket_8=3")
            .withRecordCount(2)
            .build();

    // Add file with new spec
    restTable.newAppend().appendFile(fileWithNewSpec).commit();
    table.refresh();

    // Verify snapshots - 2 snapshots total (updateSpec doesn't create a snapshot)
    assertThat(table.snapshots()).hasSize(2);
    Snapshot latestSnapshot = table.currentSnapshot();
    assertThat(latestSnapshot.addedDataFiles(table.io())).hasSize(1);
  }

  // ==================== Sequence Number Tests ====================

  @Test
  public void testSequenceNumbersIncrement() {
    Table table = createTable("sequence_numbers");
    setParserContext(table);

    RESTTable restTable = asRESTTable(table);

    // First append
    restTable.newAppend().appendFile(FILE_A).commit();
    table.refresh();
    long seq1 = table.currentSnapshot().sequenceNumber();

    // Second append
    restTable.newAppend().appendFile(FILE_B).commit();
    table.refresh();
    long seq2 = table.currentSnapshot().sequenceNumber();

    // Third append
    restTable.newAppend().appendFile(FILE_C).commit();
    table.refresh();
    long seq3 = table.currentSnapshot().sequenceNumber();

    assertThat(seq1).isGreaterThan(0);
    assertThat(seq2).isGreaterThan(seq1);
    assertThat(seq3).isGreaterThan(seq2);
  }

  // ==================== Parent Snapshot Tracking Tests ====================

  @Test
  public void testParentSnapshotTracking() {
    Table table = createTable("parent_tracking");
    setParserContext(table);

    RESTTable restTable = asRESTTable(table);

    // First append
    restTable.newAppend().appendFile(FILE_A).commit();
    table.refresh();
    long snapshot1 = table.currentSnapshot().snapshotId();
    assertThat(table.currentSnapshot().parentId()).isNull();

    // Second append
    restTable.newAppend().appendFile(FILE_B).commit();
    table.refresh();
    long snapshot2 = table.currentSnapshot().snapshotId();
    assertThat(table.currentSnapshot().parentId()).isEqualTo(snapshot1);

    // Third append
    restTable.newAppend().appendFile(FILE_C).commit();
    table.refresh();
    assertThat(table.currentSnapshot().parentId()).isEqualTo(snapshot2);
  }

  // ==================== Endpoint Support Tests ====================

  /** Helper class to hold catalog and adapter for endpoint support tests. */
  private static class CatalogWithAdapter {
    final RESTCatalog catalog;
    final RESTCatalogAdapter adapter;

    CatalogWithAdapter(RESTCatalog catalog, RESTCatalogAdapter adapter) {
      this.catalog = catalog;
      this.adapter = adapter;
    }
  }

  private List<Endpoint> baseCatalogEndpoints() {
    return ImmutableList.of(
        Endpoint.V1_CREATE_NAMESPACE,
        Endpoint.V1_LOAD_NAMESPACE,
        Endpoint.V1_LIST_TABLES,
        Endpoint.V1_CREATE_TABLE,
        Endpoint.V1_LOAD_TABLE,
        Endpoint.V1_UPDATE_TABLE);
  }

  private CatalogWithAdapter catalogWithEndpoints(List<Endpoint> endpoints) {
    RESTCatalogAdapter adapter =
        Mockito.spy(
            new RESTCatalogAdapter(backendCatalog) {
              @Override
              public <T extends RESTResponse> T execute(
                  HTTPRequest request,
                  Class<T> responseType,
                  Consumer<ErrorResponse> errorHandler,
                  Consumer<Map<String, String>> responseHeaders) {
                if (ResourcePaths.config().equals(request.path())) {
                  return castResponse(
                      responseType, ConfigResponse.builder().withEndpoints(endpoints).build());
                }
                return super.execute(request, responseType, errorHandler, responseHeaders);
              }
            });

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "test",
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO",
            RESTCatalogProperties.REST_SERVER_SIDE_COMMITS_ENABLED,
            "true"));
    return new CatalogWithAdapter(catalog, adapter);
  }

  @Test
  public void testServerSideCommitsDisabled() throws IOException {
    // When server-side commits is disabled, should use standard client-side commits
    RESTCatalog catalogWithDisabled =
        initCatalog(
            "disabled-server-commits",
            ImmutableMap.of(RESTCatalogProperties.REST_SERVER_SIDE_COMMITS_ENABLED, "false"));

    try {
      catalogWithDisabled.createNamespace(Namespace.of("test-ns"));
      Table table =
          catalogWithDisabled
              .buildTable(TableIdentifier.of("test-ns", "test-table"), SCHEMA)
              .withPartitionSpec(SPEC)
              .create();

      // When server-side commits is disabled, table should not be RESTTable
      // or if it is RESTTable, newAppend() should return the base implementation
      table.newAppend().appendFile(FILE_A).commit();
      table.refresh();

      // Verify the commit worked (regardless of which path was used)
      assertThat(table.currentSnapshot()).isNotNull();
      assertThat(table.currentSnapshot().addedDataFiles(table.io())).hasSize(1);
    } finally {
      catalogWithDisabled.close();
    }
  }

  @Test
  public void testConcurrentAppends() {
    // Test that multiple appends from same client work correctly
    Table table = createTable("concurrent_appends");
    setParserContext(table);

    RESTTable restTable = asRESTTable(table);

    // Simulate rapid appends
    restTable.newAppend().appendFile(FILE_A).commit();
    table.refresh();

    restTable.newAppend().appendFile(FILE_B).commit();
    table.refresh();

    restTable.newAppend().appendFile(FILE_C).commit();
    table.refresh();

    assertThat(table.snapshots()).hasSize(3);

    // Verify all files are present in the table
    List<DataFile> allFiles =
        org.apache.iceberg.relocated.com.google.common.collect.Lists.newArrayList(
            table.newScan().planFiles().iterator())
            .stream()
            .map(task -> task.file())
            .collect(Collectors.toList());

    assertThat(allFiles).hasSize(3);
    assertThat(allFiles)
        .extracting(DataFile::location)
        .containsExactlyInAnyOrder(FILE_A.location(), FILE_B.location(), FILE_C.location());
  }
}
