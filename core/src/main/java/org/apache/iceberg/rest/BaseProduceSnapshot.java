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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PendingUpdate;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Base class for client-side snapshot operations that produce {@link
 * MetadataUpdate.ProduceSnapshotUpdate}.
 *
 * <p>Unlike the standard snapshot producers (FastAppend, MergeAppend, etc.) which create snapshots
 * client-side and send AddSnapshot updates, this class sends file-level changes to the server
 * which then creates the snapshot. This enables:
 *
 * <ul>
 *   <li>Lightweight clients that don't need manifest writing capabilities
 *   <li>Server-side validation and conflict resolution
 *   <li>Intent-aware commits for downstream processing
 * </ul>
 *
 * @param <ThisT> the child Java API class, returned by method chaining
 */
abstract class BaseProduceSnapshot<ThisT> implements SnapshotUpdate<ThisT>, PendingUpdate<Snapshot> {

  private final TableOperations ops;
  private final String tableName;
  private final Map<String, String> summary = Maps.newHashMap();
  private final List<DataFile> addDataFiles = Lists.newArrayList();
  private final List<DeleteFile> addDeleteFiles = Lists.newArrayList();
  private final List<DataFile> removeDataFiles = Lists.newArrayList();
  private final List<DeleteFile> removeDeleteFiles = Lists.newArrayList();
  private final List<CommitValidation> validations = Lists.newArrayList();

  private Expression deleteRowFilter = null;
  private boolean stageOnly = false;
  private String targetBranch = SnapshotRef.MAIN_BRANCH;
  private Long baseSnapshotId = null;

  protected BaseProduceSnapshot(String tableName, TableOperations ops) {
    this.tableName = Preconditions.checkNotNull(tableName, "Table name cannot be null");
    this.ops = Preconditions.checkNotNull(ops, "Table operations cannot be null");
  }

  protected abstract ThisT self();

  /** Returns the snapshot operation type (append, overwrite, delete, replace). */
  protected abstract String operation();

  protected TableOperations ops() {
    return ops;
  }

  protected TableMetadata current() {
    return ops.current();
  }

  protected String tableName() {
    return tableName;
  }

  protected PartitionSpec spec(int specId) {
    return current().spec(specId);
  }

  // File addition methods

  protected void addDataFile(DataFile file) {
    Preconditions.checkNotNull(file, "Data file cannot be null");
    Preconditions.checkArgument(
        spec(file.specId()) != null,
        "Cannot find partition spec %s for data file: %s",
        file.specId(),
        file.location());
    addDataFiles.add(file);
  }

  protected void addDeleteFile(DeleteFile file) {
    Preconditions.checkNotNull(file, "Delete file cannot be null");
    Preconditions.checkArgument(
        spec(file.specId()) != null,
        "Cannot find partition spec %s for delete file: %s",
        file.specId(),
        file.location());
    addDeleteFiles.add(file);
  }

  protected void removeDataFile(DataFile file) {
    Preconditions.checkNotNull(file, "Data file cannot be null");
    removeDataFiles.add(file);
  }

  protected void removeDeleteFile(DeleteFile file) {
    Preconditions.checkNotNull(file, "Delete file cannot be null");
    removeDeleteFiles.add(file);
  }

  protected void setDeleteRowFilter(Expression filter) {
    this.deleteRowFilter = filter;
  }

  protected Expression deleteRowFilter() {
    return deleteRowFilter;
  }

  protected List<DataFile> addDataFiles() {
    return Collections.unmodifiableList(addDataFiles);
  }

  protected List<DeleteFile> addDeleteFiles() {
    return Collections.unmodifiableList(addDeleteFiles);
  }

  protected List<DataFile> removeDataFiles() {
    return Collections.unmodifiableList(removeDataFiles);
  }

  protected List<DeleteFile> removeDeleteFiles() {
    return Collections.unmodifiableList(removeDeleteFiles);
  }

  // Validation methods

  protected void addValidation(CommitValidation validation) {
    Preconditions.checkNotNull(validation, "Validation cannot be null");
    validations.add(validation);
  }

  protected List<CommitValidation> validations() {
    return validations;
  }

  // SnapshotUpdate interface methods

  @Override
  public ThisT set(String property, String value) {
    summary.put(property, value);
    return self();
  }

  @Override
  public ThisT deleteWith(Consumer<String> deleteFunc) {
    // Not needed for server-side snapshot production
    // The server handles file deletion during compaction/cleanup
    return self();
  }

  @Override
  public ThisT stageOnly() {
    this.stageOnly = true;
    return self();
  }

  @Override
  public ThisT scanManifestsWith(ExecutorService executorService) {
    // Not applicable for server-side snapshot production
    return self();
  }

  @Override
  public ThisT toBranch(String branch) {
    Preconditions.checkNotNull(branch, "Branch name cannot be null");
    this.targetBranch = branch;
    return self();
  }

  protected String targetBranch() {
    return targetBranch;
  }

  protected void setBaseSnapshotId(Long snapshotId) {
    this.baseSnapshotId = snapshotId;
  }

  protected Long baseSnapshotId() {
    return baseSnapshotId;
  }

  protected Map<String, String> summaryProperties() {
    return summary;
  }

  protected boolean isStageOnly() {
    return stageOnly;
  }

  // PendingUpdate interface methods

  @Override
  public Snapshot apply() {
    // For ProduceSnapshotUpdate, the snapshot is created server-side
    // This method returns null as the snapshot is not available until commit
    throw new UnsupportedOperationException(
        "ProduceSnapshot operations create snapshots server-side. "
            + "Use commit() to send changes to the server.");
  }

  @Override
  public void commit() {
    TableMetadata base = current();

    // Determine base snapshot ID if not set
    Long effectiveBaseSnapshotId = baseSnapshotId;
    if (effectiveBaseSnapshotId == null) {
      SnapshotRef branchRef = base.ref(targetBranch);
      if (branchRef != null) {
        effectiveBaseSnapshotId = branchRef.snapshotId();
      }
    }

    // Build the ProduceSnapshotUpdate
    MetadataUpdate.ProduceSnapshotUpdate update =
        new MetadataUpdate.ProduceSnapshotUpdate(
            operation(),
            ImmutableList.copyOf(addDataFiles),
            ImmutableList.copyOf(addDeleteFiles),
            ImmutableList.copyOf(removeDataFiles),
            ImmutableList.copyOf(removeDeleteFiles),
            deleteRowFilter,
            stageOnly,
            targetBranch,
            summary.isEmpty() ? null : Maps.newHashMap(summary),
            effectiveBaseSnapshotId,
            validations.isEmpty() ? null : ImmutableList.copyOf(validations));

    // Build the updated metadata with the ProduceSnapshotUpdate
    TableMetadata updated =
        TableMetadata.buildFrom(base).produceSnapshot(update).build();

    // Commit to the server
    // The changes list in 'updated' contains the ProduceSnapshotUpdate which
    // will be serialized and sent to the server
    ops.commit(base, updated);
  }

  @Override
  public Object updateEvent() {
    // Return null - the server will generate the appropriate event
    return null;
  }
}
