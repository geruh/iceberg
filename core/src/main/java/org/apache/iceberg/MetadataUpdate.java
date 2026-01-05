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
package org.apache.iceberg;

import java.io.Serializable;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewVersion;

/** Represents a change to table or view metadata. */
public interface MetadataUpdate extends Serializable {
  default void applyTo(TableMetadata.Builder metadataBuilder) {
    throw new UnsupportedOperationException(
        String.format("Cannot apply update %s to a table", this.getClass().getSimpleName()));
  }

  default void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
    throw new UnsupportedOperationException(
        String.format("Cannot apply update %s to a view", this.getClass().getSimpleName()));
  }

  class AssignUUID implements MetadataUpdate {
    private final String uuid;

    public AssignUUID(String uuid) {
      this.uuid = uuid;
    }

    public String uuid() {
      return uuid;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.assignUUID(uuid);
    }

    @Override
    public void applyTo(ViewMetadata.Builder metadataBuilder) {
      metadataBuilder.assignUUID(uuid);
    }
  }

  class UpgradeFormatVersion implements MetadataUpdate {
    private final int formatVersion;

    public UpgradeFormatVersion(int formatVersion) {
      this.formatVersion = formatVersion;
    }

    public int formatVersion() {
      return formatVersion;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.upgradeFormatVersion(formatVersion);
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.upgradeFormatVersion(formatVersion);
    }
  }

  class AddSchema implements MetadataUpdate {
    private final Schema schema;

    public AddSchema(Schema schema) {
      this.schema = schema;
    }

    public Schema schema() {
      return schema;
    }

    public int lastColumnId() {
      return schema.highestFieldId();
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.addSchema(schema);
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.addSchema(schema);
    }
  }

  class SetCurrentSchema implements MetadataUpdate {
    private final int schemaId;

    public SetCurrentSchema(int schemaId) {
      this.schemaId = schemaId;
    }

    public int schemaId() {
      return schemaId;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setCurrentSchema(schemaId);
    }
  }

  class AddPartitionSpec implements MetadataUpdate {
    private final UnboundPartitionSpec spec;

    public AddPartitionSpec(PartitionSpec spec) {
      this(spec.toUnbound());
    }

    public AddPartitionSpec(UnboundPartitionSpec spec) {
      this.spec = spec;
    }

    public UnboundPartitionSpec spec() {
      return spec;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.addPartitionSpec(spec);
    }
  }

  class SetDefaultPartitionSpec implements MetadataUpdate {
    private final int specId;

    public SetDefaultPartitionSpec(int specId) {
      this.specId = specId;
    }

    public int specId() {
      return specId;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setDefaultPartitionSpec(specId);
    }
  }

  class RemovePartitionSpecs implements MetadataUpdate {
    private final Set<Integer> specIds;

    public RemovePartitionSpecs(Set<Integer> specIds) {
      this.specIds = specIds;
    }

    public Set<Integer> specIds() {
      return specIds;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removeSpecs(specIds);
    }
  }

  class RemoveSchemas implements MetadataUpdate {
    private final Set<Integer> schemaIds;

    public RemoveSchemas(Set<Integer> schemaIds) {
      this.schemaIds = schemaIds;
    }

    public Set<Integer> schemaIds() {
      return schemaIds;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removeSchemas(schemaIds);
    }
  }

  class AddSortOrder implements MetadataUpdate {
    private final UnboundSortOrder sortOrder;

    public AddSortOrder(SortOrder sortOrder) {
      this(sortOrder.toUnbound());
    }

    public AddSortOrder(UnboundSortOrder sortOrder) {
      this.sortOrder = sortOrder;
    }

    public UnboundSortOrder sortOrder() {
      return sortOrder;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.addSortOrder(sortOrder);
    }
  }

  class SetDefaultSortOrder implements MetadataUpdate {
    private final int sortOrderId;

    public SetDefaultSortOrder(int sortOrderId) {
      this.sortOrderId = sortOrderId;
    }

    public int sortOrderId() {
      return sortOrderId;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setDefaultSortOrder(sortOrderId);
    }
  }

  class SetStatistics implements MetadataUpdate {
    private final StatisticsFile statisticsFile;

    public SetStatistics(StatisticsFile statisticsFile) {
      this.statisticsFile = statisticsFile;
    }

    public long snapshotId() {
      return statisticsFile.snapshotId();
    }

    public StatisticsFile statisticsFile() {
      return statisticsFile;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setStatistics(statisticsFile);
    }
  }

  class RemoveStatistics implements MetadataUpdate {
    private final long snapshotId;

    public RemoveStatistics(long snapshotId) {
      this.snapshotId = snapshotId;
    }

    public long snapshotId() {
      return snapshotId;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removeStatistics(snapshotId);
    }
  }

  class SetPartitionStatistics implements MetadataUpdate {
    private final PartitionStatisticsFile partitionStatisticsFile;

    public SetPartitionStatistics(PartitionStatisticsFile partitionStatisticsFile) {
      this.partitionStatisticsFile = partitionStatisticsFile;
    }

    public long snapshotId() {
      return partitionStatisticsFile.snapshotId();
    }

    public PartitionStatisticsFile partitionStatisticsFile() {
      return partitionStatisticsFile;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setPartitionStatistics(partitionStatisticsFile);
    }
  }

  class RemovePartitionStatistics implements MetadataUpdate {
    private final long snapshotId;

    public RemovePartitionStatistics(long snapshotId) {
      this.snapshotId = snapshotId;
    }

    public long snapshotId() {
      return snapshotId;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removePartitionStatistics(snapshotId);
    }
  }

  class AddSnapshot implements MetadataUpdate {
    private final Snapshot snapshot;

    public AddSnapshot(Snapshot snapshot) {
      this.snapshot = snapshot;
    }

    public Snapshot snapshot() {
      return snapshot;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.addSnapshot(snapshot);
    }
  }

  class RemoveSnapshots implements MetadataUpdate {
    private final Set<Long> snapshotIds;

    public RemoveSnapshots(long snapshotId) {
      this.snapshotIds = ImmutableSet.of(snapshotId);
    }

    public RemoveSnapshots(Set<Long> snapshotIds) {
      this.snapshotIds = snapshotIds;
    }

    public Set<Long> snapshotIds() {
      return snapshotIds;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removeSnapshots(snapshotIds);
    }
  }

  class RemoveSnapshotRef implements MetadataUpdate {
    private final String refName;

    public RemoveSnapshotRef(String refName) {
      this.refName = refName;
    }

    public String name() {
      return refName;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removeRef(refName);
    }
  }

  class SetSnapshotRef implements MetadataUpdate {
    private final String refName;
    private final Long snapshotId;
    private final SnapshotRefType type;
    private final Integer minSnapshotsToKeep;
    private final Long maxSnapshotAgeMs;
    private final Long maxRefAgeMs;

    public SetSnapshotRef(
        String refName,
        Long snapshotId,
        SnapshotRefType type,
        Integer minSnapshotsToKeep,
        Long maxSnapshotAgeMs,
        Long maxRefAgeMs) {
      this.refName = refName;
      this.snapshotId = snapshotId;
      this.type = type;
      this.minSnapshotsToKeep = minSnapshotsToKeep;
      this.maxSnapshotAgeMs = maxSnapshotAgeMs;
      this.maxRefAgeMs = maxRefAgeMs;
    }

    public String name() {
      return refName;
    }

    public String type() {
      return type.name().toLowerCase(Locale.ROOT);
    }

    public long snapshotId() {
      return snapshotId;
    }

    public Integer minSnapshotsToKeep() {
      return minSnapshotsToKeep;
    }

    public Long maxSnapshotAgeMs() {
      return maxSnapshotAgeMs;
    }

    public Long maxRefAgeMs() {
      return maxRefAgeMs;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      SnapshotRef ref =
          SnapshotRef.builderFor(snapshotId, type)
              .minSnapshotsToKeep(minSnapshotsToKeep)
              .maxSnapshotAgeMs(maxSnapshotAgeMs)
              .maxRefAgeMs(maxRefAgeMs)
              .build();
      metadataBuilder.setRef(refName, ref);
    }
  }

  class SetProperties implements MetadataUpdate {
    private final Map<String, String> updated;

    public SetProperties(Map<String, String> updated) {
      this.updated = updated;
    }

    public Map<String, String> updated() {
      return updated;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setProperties(updated);
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.setProperties(updated);
    }
  }

  class RemoveProperties implements MetadataUpdate {
    private final Set<String> removed;

    public RemoveProperties(Set<String> removed) {
      this.removed = removed;
    }

    public Set<String> removed() {
      return removed;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removeProperties(removed);
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.removeProperties(removed);
    }
  }

  class SetLocation implements MetadataUpdate {
    private final String location;

    public SetLocation(String location) {
      this.location = location;
    }

    public String location() {
      return location;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setLocation(location);
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.setLocation(location);
    }
  }

  class AddViewVersion implements MetadataUpdate {
    private final ViewVersion viewVersion;

    public AddViewVersion(ViewVersion viewVersion) {
      this.viewVersion = viewVersion;
    }

    public ViewVersion viewVersion() {
      return viewVersion;
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.addVersion(viewVersion);
    }
  }

  class SetCurrentViewVersion implements MetadataUpdate {
    private final int versionId;

    public SetCurrentViewVersion(int versionId) {
      this.versionId = versionId;
    }

    public int versionId() {
      return versionId;
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.setCurrentVersionId(versionId);
    }
  }

  class AddEncryptionKey implements MetadataUpdate {
    private final EncryptedKey key;

    public AddEncryptionKey(EncryptedKey key) {
      this.key = key;
    }

    public EncryptedKey key() {
      return key;
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      builder.addEncryptionKey(key);
    }
  }

  class RemoveEncryptionKey implements MetadataUpdate {
    private final String keyId;

    public RemoveEncryptionKey(String keyId) {
      this.keyId = keyId;
    }

    public String keyId() {
      return keyId;
    }

    @Override
    public void applyTo(TableMetadata.Builder builder) {
      builder.removeEncryptionKey(keyId);
    }
  }

  /**
   * A metadata update that produces a new snapshot by applying file-level changes.
   *
   * <p>This update is designed for fine-grained metadata commits where clients submit file-level
   * changes, and the catalog constructs and commits the snapshot while enforcing isolation
   * guarantees.
   *
   * <p>This class supports two modes:
   *
   * <ul>
   *   <li><b>Client-side (resolved):</b> Created with DataFile/DeleteFile objects when a client
   *       constructs the update. Use the primary constructor.
   *   <li><b>Server-side (unresolved):</b> Created during JSON parsing when partition specs are not
   *       yet available. Use {@link #createUnresolved} and later call {@link #resolve} with the
   *       table's partition specs.
   * </ul>
   */
  class ProduceSnapshotUpdate implements MetadataUpdate {
    private final String action;
    private final java.util.List<DataFile> addDataFiles;
    private final java.util.List<DeleteFile> addDeleteFiles;
    private final java.util.List<DataFile> removeDataFiles;
    private final java.util.List<DeleteFile> removeDeleteFiles;
    private final org.apache.iceberg.expressions.Expression deleteRowFilter;
    private final boolean stageOnly;
    private final String branch;
    private final java.util.Map<String, String> summary;
    private final Long baseSnapshotId;
    private final java.util.List<org.apache.iceberg.rest.CommitValidation> validations;

    // Unresolved file JSON nodes - used during parsing before specs are available
    private final java.util.List<com.fasterxml.jackson.databind.JsonNode> unresolvedAddDataFiles;
    private final java.util.List<com.fasterxml.jackson.databind.JsonNode> unresolvedAddDeleteFiles;
    private final java.util.List<com.fasterxml.jackson.databind.JsonNode> unresolvedRemoveDataFiles;
    private final java.util.List<com.fasterxml.jackson.databind.JsonNode>
        unresolvedRemoveDeleteFiles;

    /**
     * Creates a ProduceSnapshotUpdate with resolved file objects.
     *
     * <p>Use this constructor on the client side when creating an update to send to the server.
     */
    public ProduceSnapshotUpdate(
        String action,
        java.util.List<DataFile> addDataFiles,
        java.util.List<DeleteFile> addDeleteFiles,
        java.util.List<DataFile> removeDataFiles,
        java.util.List<DeleteFile> removeDeleteFiles,
        org.apache.iceberg.expressions.Expression deleteRowFilter,
        boolean stageOnly,
        String branch,
        java.util.Map<String, String> summary,
        Long baseSnapshotId,
        java.util.List<org.apache.iceberg.rest.CommitValidation> validations) {
      org.apache.iceberg.relocated.com.google.common.base.Preconditions.checkArgument(
          action != null, "Snapshot action cannot be null");
      this.action = action;
      // Defensive copy lists to prevent external mutation
      this.addDataFiles =
          addDataFiles != null
              ? org.apache.iceberg.relocated.com.google.common.collect.ImmutableList.copyOf(
                  addDataFiles)
              : java.util.Collections.emptyList();
      this.addDeleteFiles =
          addDeleteFiles != null
              ? org.apache.iceberg.relocated.com.google.common.collect.ImmutableList.copyOf(
                  addDeleteFiles)
              : java.util.Collections.emptyList();
      this.removeDataFiles =
          removeDataFiles != null
              ? org.apache.iceberg.relocated.com.google.common.collect.ImmutableList.copyOf(
                  removeDataFiles)
              : java.util.Collections.emptyList();
      this.removeDeleteFiles =
          removeDeleteFiles != null
              ? org.apache.iceberg.relocated.com.google.common.collect.ImmutableList.copyOf(
                  removeDeleteFiles)
              : java.util.Collections.emptyList();
      this.deleteRowFilter = deleteRowFilter;
      this.stageOnly = stageOnly;
      this.branch = branch;
      this.summary =
          summary != null
              ? org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap.copyOf(summary)
              : java.util.Collections.emptyMap();
      this.baseSnapshotId = baseSnapshotId;
      this.validations =
          validations != null
              ? org.apache.iceberg.relocated.com.google.common.collect.ImmutableList.copyOf(
                  validations)
              : java.util.Collections.emptyList();
      // No unresolved JSON when constructed with resolved files
      this.unresolvedAddDataFiles = null;
      this.unresolvedAddDeleteFiles = null;
      this.unresolvedRemoveDataFiles = null;
      this.unresolvedRemoveDeleteFiles = null;
    }

    // Private constructor for unresolved state (used by parser)
    private ProduceSnapshotUpdate(
        String action,
        java.util.List<com.fasterxml.jackson.databind.JsonNode> unresolvedAddDataFiles,
        java.util.List<com.fasterxml.jackson.databind.JsonNode> unresolvedAddDeleteFiles,
        java.util.List<com.fasterxml.jackson.databind.JsonNode> unresolvedRemoveDataFiles,
        java.util.List<com.fasterxml.jackson.databind.JsonNode> unresolvedRemoveDeleteFiles,
        org.apache.iceberg.expressions.Expression deleteRowFilter,
        boolean stageOnly,
        String branch,
        java.util.Map<String, String> summary,
        Long baseSnapshotId,
        java.util.List<org.apache.iceberg.rest.CommitValidation> validations,
        @SuppressWarnings("unused") boolean unresolvedMarker) {
      org.apache.iceberg.relocated.com.google.common.base.Preconditions.checkArgument(
          action != null, "Snapshot action cannot be null");
      this.action = action;
      this.addDataFiles = null;
      this.addDeleteFiles = null;
      this.removeDataFiles = null;
      this.removeDeleteFiles = null;
      this.deleteRowFilter = deleteRowFilter;
      this.stageOnly = stageOnly;
      this.branch = branch;
      this.summary =
          summary != null
              ? org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap.copyOf(summary)
              : java.util.Collections.emptyMap();
      this.baseSnapshotId = baseSnapshotId;
      this.validations =
          validations != null
              ? org.apache.iceberg.relocated.com.google.common.collect.ImmutableList.copyOf(
                  validations)
              : java.util.Collections.emptyList();
      this.unresolvedAddDataFiles = unresolvedAddDataFiles;
      this.unresolvedAddDeleteFiles = unresolvedAddDeleteFiles;
      this.unresolvedRemoveDataFiles = unresolvedRemoveDataFiles;
      this.unresolvedRemoveDeleteFiles = unresolvedRemoveDeleteFiles;
    }

    /**
     * Creates an unresolved ProduceSnapshotUpdate from JSON parsing.
     *
     * <p>The file JSON nodes are stored and can be resolved later using {@link #resolve} when
     * partition specs are available.
     */
    public static ProduceSnapshotUpdate createUnresolved(
        String action,
        java.util.List<com.fasterxml.jackson.databind.JsonNode> unresolvedAddDataFiles,
        java.util.List<com.fasterxml.jackson.databind.JsonNode> unresolvedAddDeleteFiles,
        java.util.List<com.fasterxml.jackson.databind.JsonNode> unresolvedRemoveDataFiles,
        java.util.List<com.fasterxml.jackson.databind.JsonNode> unresolvedRemoveDeleteFiles,
        org.apache.iceberg.expressions.Expression deleteRowFilter,
        boolean stageOnly,
        String branch,
        java.util.Map<String, String> summary,
        Long baseSnapshotId,
        java.util.List<org.apache.iceberg.rest.CommitValidation> validations) {
      return new ProduceSnapshotUpdate(
          action,
          unresolvedAddDataFiles,
          unresolvedAddDeleteFiles,
          unresolvedRemoveDataFiles,
          unresolvedRemoveDeleteFiles,
          deleteRowFilter,
          stageOnly,
          branch,
          summary,
          baseSnapshotId,
          validations,
          true /* unresolvedMarker */);
    }

    /**
     * Resolves the unresolved file JSON nodes using the provided partition specs.
     *
     * @param specsById map of partition spec ID to partition spec
     * @return a new ProduceSnapshotUpdate with resolved DataFile/DeleteFile objects
     * @throws IllegalStateException if this update is already resolved
     */
    public ProduceSnapshotUpdate resolve(java.util.Map<Integer, PartitionSpec> specsById) {
      if (!isUnresolved()) {
        throw new IllegalStateException("ProduceSnapshotUpdate is already resolved");
      }

      java.util.List<DataFile> resolvedAddData =
          resolveDataFiles(unresolvedAddDataFiles, specsById);
      java.util.List<DeleteFile> resolvedAddDelete =
          resolveDeleteFiles(unresolvedAddDeleteFiles, specsById);
      java.util.List<DataFile> resolvedRemoveData =
          resolveDataFiles(unresolvedRemoveDataFiles, specsById);
      java.util.List<DeleteFile> resolvedRemoveDelete =
          resolveDeleteFiles(unresolvedRemoveDeleteFiles, specsById);

      return new ProduceSnapshotUpdate(
          action,
          resolvedAddData,
          resolvedAddDelete,
          resolvedRemoveData,
          resolvedRemoveDelete,
          deleteRowFilter,
          stageOnly,
          branch,
          summary,
          baseSnapshotId,
          validations);
    }

    private java.util.List<DataFile> resolveDataFiles(
        java.util.List<com.fasterxml.jackson.databind.JsonNode> jsonNodes,
        java.util.Map<Integer, PartitionSpec> specsById) {
      if (jsonNodes == null || jsonNodes.isEmpty()) {
        return java.util.Collections.emptyList();
      }
      org.apache.iceberg.relocated.com.google.common.collect.ImmutableList.Builder<DataFile>
          builder =
              org.apache.iceberg.relocated.com.google.common.collect.ImmutableList.builder();
      for (com.fasterxml.jackson.databind.JsonNode node : jsonNodes) {
        builder.add((DataFile) ContentFileParser.fromJson(node, specsById));
      }
      return builder.build();
    }

    private java.util.List<DeleteFile> resolveDeleteFiles(
        java.util.List<com.fasterxml.jackson.databind.JsonNode> jsonNodes,
        java.util.Map<Integer, PartitionSpec> specsById) {
      if (jsonNodes == null || jsonNodes.isEmpty()) {
        return java.util.Collections.emptyList();
      }
      org.apache.iceberg.relocated.com.google.common.collect.ImmutableList.Builder<DeleteFile>
          builder =
              org.apache.iceberg.relocated.com.google.common.collect.ImmutableList.builder();
      for (com.fasterxml.jackson.databind.JsonNode node : jsonNodes) {
        builder.add((DeleteFile) ContentFileParser.fromJson(node, specsById));
      }
      return builder.build();
    }

    /**
     * Returns whether this update has unresolved file JSON that needs to be resolved.
     *
     * <p>An update is unresolved when it was created via {@link #createUnresolved} during JSON
     * parsing. In the unresolved state, the resolved file lists (addDataFiles, etc.) are null. In
     * the resolved state, they are never null (at minimum empty lists).
     *
     * @return true if unresolved, false if resolved
     */
    public boolean isUnresolved() {
      // The resolved constructor always sets addDataFiles to a non-null value (emptyList at minimum)
      // The unresolved constructor always sets addDataFiles to null
      // So we can use addDataFiles == null to detect unresolved state
      return addDataFiles == null;
    }

    public String action() {
      return action;
    }

    public java.util.List<DataFile> addDataFiles() {
      checkResolved();
      return addDataFiles;
    }

    public java.util.List<DeleteFile> addDeleteFiles() {
      checkResolved();
      return addDeleteFiles;
    }

    public java.util.List<DataFile> removeDataFiles() {
      checkResolved();
      return removeDataFiles;
    }

    public java.util.List<DeleteFile> removeDeleteFiles() {
      checkResolved();
      return removeDeleteFiles;
    }

    /**
     * Returns the unresolved add-data-files JSON nodes, or null if resolved.
     *
     * <p>This is used by MetadataUpdateParser for serialization when files are unresolved.
     */
    public java.util.List<com.fasterxml.jackson.databind.JsonNode> unresolvedAddDataFiles() {
      return unresolvedAddDataFiles;
    }

    /**
     * Returns the unresolved add-delete-files JSON nodes, or null if resolved.
     *
     * <p>This is used by MetadataUpdateParser for serialization when files are unresolved.
     */
    public java.util.List<com.fasterxml.jackson.databind.JsonNode> unresolvedAddDeleteFiles() {
      return unresolvedAddDeleteFiles;
    }

    /**
     * Returns the unresolved remove-data-files JSON nodes, or null if resolved.
     *
     * <p>This is used by MetadataUpdateParser for serialization when files are unresolved.
     */
    public java.util.List<com.fasterxml.jackson.databind.JsonNode> unresolvedRemoveDataFiles() {
      return unresolvedRemoveDataFiles;
    }

    /**
     * Returns the unresolved remove-delete-files JSON nodes, or null if resolved.
     *
     * <p>This is used by MetadataUpdateParser for serialization when files are unresolved.
     */
    public java.util.List<com.fasterxml.jackson.databind.JsonNode> unresolvedRemoveDeleteFiles() {
      return unresolvedRemoveDeleteFiles;
    }

    private void checkResolved() {
      if (isUnresolved()) {
        throw new IllegalStateException(
            "ProduceSnapshotUpdate has unresolved files. Call resolve(specsById) first.");
      }
    }

    public org.apache.iceberg.expressions.Expression deleteRowFilter() {
      return deleteRowFilter;
    }

    public boolean stageOnly() {
      return stageOnly;
    }

    public String branch() {
      return branch;
    }

    public java.util.Map<String, String> summary() {
      return summary;
    }

    public Long baseSnapshotId() {
      return baseSnapshotId;
    }

    public java.util.List<org.apache.iceberg.rest.CommitValidation> validations() {
      return validations;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      // ProduceSnapshotUpdate is processed by the server-side handler in CatalogHandlers
      // The actual snapshot production is done there, not in this method
      throw new UnsupportedOperationException(
          "ProduceSnapshotUpdate must be processed by the server-side handler");
    }
  }
}
