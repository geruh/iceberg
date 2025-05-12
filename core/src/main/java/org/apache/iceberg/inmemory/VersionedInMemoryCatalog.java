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
package org.apache.iceberg.inmemory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.BaseCatalogTransaction;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsCatalogTransactions;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.view.BaseMetastoreViewCatalog;
import org.apache.iceberg.view.BaseViewOperations;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.apache.iceberg.view.ViewUtil;

/**
 * Catalog implementation that uses in-memory data-structures to store the namespaces and tables.
 * This class doesn't touch external resources and can be utilized to write unit tests without side
 * effects. It uses {@link InMemoryFileIO}.
 */
public class VersionedInMemoryCatalog extends BaseMetastoreViewCatalog
    implements SupportsNamespaces, SupportsCatalogTransactions, Closeable {
  private static final Joiner SLASH = Joiner.on("/");
  private static final Joiner DOT = Joiner.on(".");

  private final ConcurrentMap<Namespace, Map<String, String>> namespaces;
  private final ConcurrentMap<TableIdentifier, String> views;
  private final AtomicLong catalogSequence;
  private final ConcurrentMap<TableIdentifier, List<VersionedTableEntry>> tables;
  private FileIO io;
  private String catalogName;
  private String warehouseLocation;

  public VersionedInMemoryCatalog() {
    this.namespaces = Maps.newConcurrentMap();
    this.tables = Maps.newConcurrentMap();
    this.views = Maps.newConcurrentMap();
    this.catalogSequence = new AtomicLong(0);
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.catalogName = name != null ? name : VersionedInMemoryCatalog.class.getSimpleName();
    String warehouse = properties.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "");
    this.warehouseLocation = warehouse.replaceAll("/*$", "");
    this.io = new InMemoryFileIO();
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return newTableOps(tableIdentifier, null);
  }

  private TableOperations newTableOps(TableIdentifier tableIdentifier, Long sequenceNumber) {
    return new VersionedInMemoryTableOperations(io, tableIdentifier, sequenceNumber);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return SLASH.join(
        defaultNamespaceLocation(tableIdentifier.namespace()), tableIdentifier.name());
  }

  private String defaultNamespaceLocation(Namespace namespace) {
    if (namespace.isEmpty()) {
      return warehouseLocation;
    } else {
      return SLASH.join(warehouseLocation, SLASH.join(namespace.levels()));
    }
  }

  @Override
  public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
    List<VersionedTableEntry> versions = tables.get(tableIdentifier);
    if (versions == null || versions.isEmpty()) {
      return false;
    }

    long deleteSeq = catalogSequence.incrementAndGet();
    VersionedTableEntry last = versions.get(versions.size() - 1);
    last.markDeleted(deleteSeq);

    TableOperations ops = newTableOps(tableIdentifier);
    TableMetadata lastMetadata;
    if (purge && ops.current() != null) {
      lastMetadata = ops.current();
    } else {
      lastMetadata = null;
    }

    synchronized (this) {
      if (null == tables.remove(tableIdentifier)) {
        return false;
      }
    }

    if (purge && lastMetadata != null) {
      CatalogUtil.dropTableData(ops.io(), lastMetadata);
    }

    return true;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    if (!namespaceExists(namespace) && !namespace.isEmpty()) {
      throw new NoSuchNamespaceException(
          "Cannot list tables for namespace. Namespace does not exist: %s", namespace);
    }

    return tables.keySet().stream()
        .filter(t -> namespace.isEmpty() || t.namespace().equals(namespace))
        .sorted(Comparator.comparing(TableIdentifier::toString))
        .collect(Collectors.toList());
  }

  @Override
  public Long catalogSequenceNumber() {
    return catalogSequence.get();
  }

  @Override
  public Table loadTable(TableIdentifier identifier, long sequenceNumber) {
    if (!isValidIdentifier(identifier)) {
      throw new NoSuchTableException("Invalid table identifier: %s", identifier);
    }

    List<VersionedTableEntry> versions = tables.get(identifier);
    if (versions == null || versions.isEmpty()) {
      throw new NoSuchTableException("Table does not exist: %s", identifier);
    }

    VersionedTableEntry entry =
        versions.stream()
            .filter(v -> v.isValidAt(sequenceNumber))
            .findFirst()
            .orElseThrow(() -> new NoSuchTableException("Table does not exist: %s", identifier));

    TableOperations ops = newTableOps(identifier, sequenceNumber);
    if (ops.current() == null) {
      throw new NoSuchTableException("Table does not exist: %s", identifier);
    }

    return new BaseTable(
        ops, fullTableName(name(), identifier), CatalogUtil.loadMetricsReporter(properties()));
  }

  @Override
  public CatalogTransaction beginTransaction(IsolationLevel isolationLevel) {
    return new BaseCatalogTransaction(this, isolationLevel);
  }

  @Override
  public void multiTableCommit(List<TableCommit> commits) {
    synchronized (this) {
      for (TableCommit commit : commits) {
        TableIdentifier tableIdentifier = commit.identifier();
        TableOperations ops = newTableOps(tableIdentifier);
        TableMetadata current = ops.current();

        boolean isCreate =
            commit.requirements().stream()
                .anyMatch(req -> req instanceof UpdateRequirement.AssertTableDoesNotExist);

        if (!isCreate && current == null) {
          throw new NoSuchTableException("Table does not exist: %s", tableIdentifier);
        }

        for (UpdateRequirement req : commit.requirements()) {
          req.validate(current);
        }
      }

      // One sequence for the whole batch
      long newSeq = catalogSequence.incrementAndGet();

      for (TableCommit commit : commits) {
        TableIdentifier tableIdentifier = commit.identifier();
        VersionedInMemoryTableOperations ops =
            (VersionedInMemoryTableOperations) newTableOps(tableIdentifier);
        TableMetadata current = ops.current();

        TableMetadata.Builder builder =
            current == null ? TableMetadata.buildFromEmpty() : TableMetadata.buildFrom(current);
        for (MetadataUpdate update : commit.updates()) {
          update.applyTo(builder);
        }
        TableMetadata newMetadata = builder.build();
        ops.doCommit(current, newMetadata, newSeq);
      }
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    if (from.equals(to)) {
      return;
    }

    synchronized (this) {
      if (!namespaceExists(to.namespace())) {
        throw new NoSuchNamespaceException(
            "Cannot rename %s to %s. Namespace does not exist: %s", from, to, to.namespace());
      }

      List<VersionedTableEntry> fromVersions = tables.get(from);
      if (fromVersions == null || fromVersions.isEmpty()) {
        throw new NoSuchTableException("Cannot rename %s to %s. Table does not exist", from, to);
      }

      if (tables.containsKey(to)) {
        throw new AlreadyExistsException("Cannot rename %s to %s. Table already exists", from, to);
      }

      if (views.containsKey(to)) {
        throw new AlreadyExistsException("Cannot rename %s to %s. View already exists", from, to);
      }

      tables.put(to, fromVersions);
      tables.remove(from);
    }
  }

  @Override
  public void createNamespace(Namespace namespace) {
    createNamespace(namespace, Collections.emptyMap());
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    synchronized (this) {
      if (namespaceExists(namespace)) {
        throw new AlreadyExistsException(
            "Cannot create namespace %s. Namespace already exists", namespace);
      }

      namespaces.put(namespace, ImmutableMap.copyOf(metadata));
    }
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    return namespaces.containsKey(namespace);
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    synchronized (this) {
      if (!namespaceExists(namespace)) {
        return false;
      }

      List<TableIdentifier> tableIdentifiers = listTables(namespace);
      if (!tableIdentifiers.isEmpty()) {
        throw new NamespaceNotEmptyException(
            "Namespace %s is not empty. Contains %d table(s).", namespace, tableIdentifiers.size());
      }

      return namespaces.remove(namespace) != null;
    }
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    synchronized (this) {
      if (!namespaceExists(namespace)) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }

      namespaces.computeIfPresent(
          namespace,
          (k, v) ->
              ImmutableMap.<String, String>builder()
                  .putAll(v)
                  .putAll(properties)
                  .buildKeepingLast());

      return true;
    }
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    synchronized (this) {
      if (!namespaceExists(namespace)) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }

      namespaces.computeIfPresent(
          namespace,
          (k, v) -> {
            Map<String, String> newProperties = Maps.newHashMap(v);
            properties.forEach(newProperties::remove);
            return ImmutableMap.copyOf(newProperties);
          });

      return true;
    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    Map<String, String> properties = namespaces.get(namespace);
    if (properties == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    return ImmutableMap.copyOf(properties);
  }

  @Override
  public List<Namespace> listNamespaces() {
    return namespaces.keySet().stream()
        .filter(n -> !n.isEmpty())
        .map(n -> n.level(0))
        .distinct()
        .sorted()
        .map(Namespace::of)
        .collect(Collectors.toList());
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    final String searchNamespaceString =
        namespace.isEmpty() ? "" : DOT.join(namespace.levels()) + ".";
    final int searchNumberOfLevels = namespace.levels().length;

    List<Namespace> filteredNamespaces =
        namespaces.keySet().stream()
            .filter(n -> DOT.join(n.levels()).startsWith(searchNamespaceString))
            .collect(Collectors.toList());

    // If the namespace does not exist and the namespace is not a prefix of another namespace,
    // throw an exception.
    if (!namespaces.containsKey(namespace) && filteredNamespaces.isEmpty()) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    return filteredNamespaces.stream()
        // List only the child-namespaces roots.
        .map(n -> Namespace.of(Arrays.copyOf(n.levels(), searchNumberOfLevels + 1)))
        .distinct()
        .sorted(Comparator.comparing(n -> DOT.join(n.levels())))
        .collect(Collectors.toList());
  }

  @Override
  public void close() throws IOException {
    namespaces.clear();
    tables.clear();
    views.clear();
  }

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    if (!namespaceExists(namespace) && !namespace.isEmpty()) {
      throw new NoSuchNamespaceException(
          "Cannot list views for namespace. Namespace does not exist: %s", namespace);
    }

    return views.keySet().stream()
        .filter(v -> namespace.isEmpty() || v.namespace().equals(namespace))
        .sorted(Comparator.comparing(TableIdentifier::toString))
        .collect(Collectors.toList());
  }

  @Override
  protected ViewOperations newViewOps(TableIdentifier identifier) {
    return new InMemoryViewOperations(io, identifier);
  }

  @Override
  public boolean dropView(TableIdentifier identifier) {
    synchronized (this) {
      return null != views.remove(identifier);
    }
  }

  @Override
  public void renameView(TableIdentifier from, TableIdentifier to) {
    if (from.equals(to)) {
      return;
    }

    synchronized (this) {
      if (!namespaceExists(to.namespace())) {
        throw new NoSuchNamespaceException(
            "Cannot rename %s to %s. Namespace does not exist: %s", from, to, to.namespace());
      }

      String fromViewLocation = views.get(from);
      if (null == fromViewLocation) {
        throw new NoSuchViewException("Cannot rename %s to %s. View does not exist", from, to);
      }

      if (tables.containsKey(to)) {
        throw new AlreadyExistsException("Cannot rename %s to %s. Table already exists", from, to);
      }

      if (views.containsKey(to)) {
        throw new AlreadyExistsException("Cannot rename %s to %s. View already exists", from, to);
      }

      views.put(to, fromViewLocation);
      views.remove(from);
    }
  }

  private class VersionedInMemoryTableOperations extends BaseMetastoreTableOperations {
    private final FileIO fileIO;
    private final TableIdentifier tableIdentifier;
    private final String fullTableName;
    private Long pinnedSequence;

    VersionedInMemoryTableOperations(FileIO fileIO, TableIdentifier tableIdentifier) {
      this(fileIO, tableIdentifier, null);
    }

    VersionedInMemoryTableOperations(
        FileIO fileIO, TableIdentifier tableIdentifier, Long pinnedSequence) {
      this.fileIO = fileIO;
      this.tableIdentifier = tableIdentifier;
      this.fullTableName = fullTableName(catalogName, tableIdentifier);
      this.pinnedSequence = pinnedSequence;
    }

    @Override
    public void doRefresh() {
      List<VersionedTableEntry> versions = tables.get(tableIdentifier);
      if (versions == null || versions.isEmpty()) {
        disableRefresh();
        return;
      }

      VersionedTableEntry entry;
      // can catch table not found here
      if (this.pinnedSequence != null) {
        // Find the version valid at the pinned sequence number
        entry = versions.stream().filter(v -> v.isValidAt(pinnedSequence)).findFirst().orElse(null);
        // future refreshes use latest
        this.pinnedSequence = null;
      } else {
        // Get the latest version
        entry = versions.get(versions.size() - 1);
        if (entry.xMax() != Long.MAX_VALUE) {
          entry = null;
        }
      }

      if (entry == null) {
        disableRefresh();
      } else {
        refreshFromMetadataLocation(entry.location());
      }
    }

    public void doCommit(TableMetadata base, TableMetadata metadata, Long sequenceNumber) {
      String newLocation = writeNewMetadataIfRequired(base == null, metadata);
      String oldLocation = base == null ? null : base.metadataFileLocation();

      synchronized (VersionedInMemoryCatalog.this) {
        if (null == base && !namespaceExists(tableIdentifier.namespace())) {
          throw new NoSuchNamespaceException(
              "Cannot create table %s. Namespace does not exist: %s",
              tableIdentifier, tableIdentifier.namespace());
        }

        if (views.containsKey(tableIdentifier)) {
          throw new AlreadyExistsException(
              "View with same name already exists: %s", tableIdentifier);
        }

        tables.compute(
            tableIdentifier,
            (k, oldVersions) -> {
              List<VersionedTableEntry> versions = oldVersions;
              if (versions == null || versions.isEmpty()) {
                versions = Lists.newArrayList();
              }

              if (!versions.isEmpty()) {
                VersionedTableEntry latest = versions.get(versions.size() - 1);
                if (!Objects.equal(latest.location(), oldLocation)) {
                  if (null == base) {
                    throw new AlreadyExistsException("Table already exists: %s", tableName());
                  }
                  if (null == latest.location()) {
                    throw new NoSuchTableException("Table does not exist: %s", tableName());
                  }
                  throw new CommitFailedException(
                      "Cannot commit to table %s metadata location from %s to %s "
                          + "because it has been concurrently modified to %s",
                      tableIdentifier, oldLocation, newLocation, latest.location());
                }
                latest.invalidate(sequenceNumber);
              }
              versions.add(new VersionedTableEntry(newLocation, sequenceNumber));
              return versions;
            });
      }
    }

    @Override
    public void doCommit(TableMetadata base, TableMetadata metadata) {
      long newSeq = catalogSequence.incrementAndGet();
      doCommit(base, metadata, newSeq);
    }

    @Override
    public FileIO io() {
      return fileIO;
    }

    @Override
    protected String tableName() {
      return fullTableName;
    }
  }

  private class InMemoryViewOperations extends BaseViewOperations {
    private final FileIO io;
    private final TableIdentifier identifier;
    private final String fullViewName;

    InMemoryViewOperations(FileIO io, TableIdentifier identifier) {
      this.io = io;
      this.identifier = identifier;
      this.fullViewName = ViewUtil.fullViewName(catalogName, identifier);
    }

    @Override
    public void doRefresh() {
      String latestLocation = views.get(identifier);
      if (latestLocation == null) {
        disableRefresh();
      } else {
        refreshFromMetadataLocation(latestLocation);
      }
    }

    @Override
    public void doCommit(ViewMetadata base, ViewMetadata metadata) {
      String newLocation = writeNewMetadataIfRequired(metadata);
      String oldLocation = base == null ? null : currentMetadataLocation();

      synchronized (VersionedInMemoryCatalog.this) {
        if (null == base && !namespaceExists(identifier.namespace())) {
          throw new NoSuchNamespaceException(
              "Cannot create view %s. Namespace does not exist: %s",
              identifier, identifier.namespace());
        }

        if (tables.containsKey(identifier)) {
          throw new AlreadyExistsException("Table with same name already exists: %s", identifier);
        }

        views.compute(
            identifier,
            (k, existingLocation) -> {
              if (!Objects.equal(existingLocation, oldLocation)) {
                if (null == base) {
                  throw new AlreadyExistsException("View already exists: %s", identifier);
                }

                if (null == existingLocation) {
                  throw new NoSuchViewException("View does not exist: %s", identifier);
                }

                throw new CommitFailedException(
                    "Cannot commit to view %s metadata location from %s to %s "
                        + "because it has been concurrently modified to %s",
                    identifier, oldLocation, newLocation, existingLocation);
              }

              return newLocation;
            });
      }
    }

    @Override
    public FileIO io() {
      return io;
    }

    @Override
    protected String viewName() {
      return fullViewName;
    }
  }

  private static class VersionedTableEntry {
    private final String location;
    private final long xMin;
    private long xMax;

    private VersionedTableEntry(String location, long xMin) {
      this.location = location;
      this.xMin = xMin;
      this.xMax = Long.MAX_VALUE;
    }

    public String location() {
      return location;
    }

    public long xMax() {
      return xMax;
    }

    public boolean isValidAt(long sequenceNumber) {
      return sequenceNumber >= xMin && sequenceNumber < xMax;
    }

    public void invalidate(long sequenceNumber) {
      this.xMax = sequenceNumber;
    }

    public void markDeleted(long sequenceNumber) {
      this.xMax = sequenceNumber;
    }
  }
}
