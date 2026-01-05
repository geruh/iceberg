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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.CommitValidation;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for processing {@link MetadataUpdate.ProduceSnapshotUpdate} in the REST catalog.
 *
 * <p>This handler takes file-level changes from a ProduceSnapshotUpdate and constructs manifest
 * files and a snapshot. The resulting snapshot is then added to the table metadata via the metadata
 * builder.
 *
 * <p>Unlike client-side snapshot producers that use the Table API, this handler works directly with
 * TableOperations and TableMetadata to create snapshots server-side. This allows it to integrate
 * with the CatalogHandlers commit loop.
 */
public class ProduceSnapshotUpdateHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ProduceSnapshotUpdateHandler.class);

  // Operations where new data files can be added
  private static final Set<String> VALIDATE_ADDED_FILES_OPERATIONS =
      Sets.newHashSet(DataOperations.APPEND, DataOperations.OVERWRITE);

  // Operations where new delete files can be added
  private static final Set<String> VALIDATE_ADDED_DELETE_FILES_OPERATIONS =
      Sets.newHashSet(DataOperations.OVERWRITE, DataOperations.DELETE);

  private ProduceSnapshotUpdateHandler() {}

  /**
   * Resolves a ProduceSnapshotUpdate using the table's partition specs.
   *
   * <p>When a ProduceSnapshotUpdate is parsed from JSON, the file data is stored as raw JSON nodes
   * because partition specs are needed to parse them into DataFile/DeleteFile objects. This method
   * performs that resolution using the table's partition specs.
   *
   * @param update the potentially unresolved update
   * @param base the table metadata containing partition specs
   * @return the resolved update (or the same update if already resolved)
   */
  public static MetadataUpdate.ProduceSnapshotUpdate resolve(
      MetadataUpdate.ProduceSnapshotUpdate update, TableMetadata base) {
    if (!update.isUnresolved()) {
      return update;
    }

    Map<Integer, PartitionSpec> specsById = base.specsById();
    return update.resolve(specsById);
  }

  /**
   * Applies a ProduceSnapshotUpdate to the table metadata builder.
   *
   * <p>This method creates manifest files for the data and delete files, constructs a snapshot
   * pointing to those manifests, and adds it to the metadata builder.
   *
   * @param ops the table operations (provides FileIO and location)
   * @param base the current table metadata
   * @param update the resolved ProduceSnapshotUpdate
   * @param metadataBuilder the metadata builder to apply the snapshot to
   */
  public static void apply(
      TableOperations ops,
      TableMetadata base,
      MetadataUpdate.ProduceSnapshotUpdate update,
      TableMetadata.Builder metadataBuilder) {
    Preconditions.checkArgument(
        !update.isUnresolved(),
        "ProduceSnapshotUpdate must be resolved before processing. "
            + "Call resolve(update, base) first.");

    String action = update.action();
    Preconditions.checkArgument(action != null, "ProduceSnapshotUpdate action cannot be null");

    // Validate action is supported
    validateAction(action);

    // Determine target branch and parent snapshot
    String targetBranch =
        update.branch() != null ? update.branch() : SnapshotRef.MAIN_BRANCH;
    Snapshot parentSnapshot = SnapshotUtil.latestSnapshot(base, targetBranch);
    Long parentSnapshotId = parentSnapshot != null ? parentSnapshot.snapshotId() : null;

    // Determine base snapshot ID for validation
    Long baseSnapshotId = update.baseSnapshotId();
    if (baseSnapshotId == null && parentSnapshot != null) {
      baseSnapshotId = parentSnapshot.snapshotId();
    }

    // Run commit validations before creating the snapshot
    if (update.validations() != null) {
      for (CommitValidation validation : update.validations()) {
        applyValidation(ops, base, parentSnapshot, baseSnapshotId, validation);
      }
    }

    // Generate IDs and locations
    String commitUUID = UUID.randomUUID().toString();
    AtomicInteger manifestCount = new AtomicInteger(0);
    long snapshotId = ops.newSnapshotId();
    long sequenceNumber = base.nextSequenceNumber();
    int formatVersion = base.formatVersion();

    // Create manifest files for all file changes
    List<ManifestFile> manifests = Lists.newArrayList();

    // Add manifests for added data files (grouped by partition spec)
    Map<Integer, List<DataFile>> addedDataBySpec = groupBySpec(update.addDataFiles());
    for (Map.Entry<Integer, List<DataFile>> entry : addedDataBySpec.entrySet()) {
      PartitionSpec spec = base.spec(entry.getKey());
      ManifestFile manifest =
          writeDataManifest(
              ops, formatVersion, spec, entry.getValue(), snapshotId, commitUUID, manifestCount);
      manifests.add(manifest);
    }

    // Add manifests for removed data files (mark as DELETED)
    if (!update.removeDataFiles().isEmpty()) {
      Map<Integer, List<DataFile>> removedDataBySpec = groupBySpec(update.removeDataFiles());
      for (Map.Entry<Integer, List<DataFile>> entry : removedDataBySpec.entrySet()) {
        PartitionSpec spec = base.spec(entry.getKey());
        ManifestFile manifest =
            writeDataManifestWithDeleted(
                ops, formatVersion, spec, entry.getValue(), snapshotId, commitUUID, manifestCount);
        manifests.add(manifest);
      }
    }

    // Add manifests for added delete files
    if (formatVersion >= 2) {
      Map<Integer, List<DeleteFile>> addedDeletesBySpec = groupDeletesBySpec(update.addDeleteFiles());
      for (Map.Entry<Integer, List<DeleteFile>> entry : addedDeletesBySpec.entrySet()) {
        PartitionSpec spec = base.spec(entry.getKey());
        ManifestFile manifest =
            writeDeleteManifest(
                ops, formatVersion, spec, entry.getValue(), snapshotId, commitUUID, manifestCount);
        manifests.add(manifest);
      }

      // Add manifests for removed delete files (mark as DELETED)
      if (!update.removeDeleteFiles().isEmpty()) {
        Map<Integer, List<DeleteFile>> removedDeletesBySpec =
            groupDeletesBySpec(update.removeDeleteFiles());
        for (Map.Entry<Integer, List<DeleteFile>> entry : removedDeletesBySpec.entrySet()) {
          PartitionSpec spec = base.spec(entry.getKey());
          ManifestFile manifest =
              writeDeleteManifestWithDeleted(
                  ops,
                  formatVersion,
                  spec,
                  entry.getValue(),
                  snapshotId,
                  commitUUID,
                  manifestCount);
          manifests.add(manifest);
        }
      }
    }

    // Carry forward existing manifests from parent snapshot
    if (parentSnapshot != null) {
      for (ManifestFile existingManifest : parentSnapshot.allManifests(ops.io())) {
        manifests.add(existingManifest);
      }
    }

    // Write manifest list
    OutputFile manifestListFile = manifestListPath(ops, snapshotId, 1, commitUUID);
    String manifestListLocation;

    try (ManifestListWriter writer =
        ManifestLists.write(
            formatVersion,
            manifestListFile,
            ops.encryption(),
            snapshotId,
            parentSnapshotId,
            sequenceNumber,
            base.nextRowId())) {
      writer.addAll(manifests);
      manifestListLocation = manifestListFile.location();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write manifest list", e);
    }

    // Build snapshot summary
    Map<String, String> summary = buildSummary(update, parentSnapshot);

    // Create the snapshot
    Snapshot newSnapshot =
        new BaseSnapshot(
            sequenceNumber,
            snapshotId,
            parentSnapshotId,
            System.currentTimeMillis(),
            action,
            summary,
            base.currentSchemaId(),
            manifestListLocation,
            formatVersion >= 3 ? base.nextRowId() : null,
            formatVersion >= 3 ? 0L : null, // TODO: calculate actual added rows
            null);

    // Add snapshot to metadata
    if (update.stageOnly()) {
      metadataBuilder.addSnapshot(newSnapshot);
    } else {
      metadataBuilder.setBranchSnapshot(newSnapshot, targetBranch);
    }
  }

  private static void validateAction(String action) {
    String lowerAction = action.toLowerCase(Locale.ROOT);
    switch (lowerAction) {
      case DataOperations.APPEND:
      case DataOperations.OVERWRITE:
      case DataOperations.DELETE:
      case DataOperations.REPLACE:
        break;
      default:
        throw new IllegalArgumentException("Unsupported ProduceSnapshotUpdate action: " + action);
    }
  }

  private static <T extends org.apache.iceberg.ContentFile<?>> Map<Integer, List<T>> groupBySpecGeneric(
      List<T> files) {
    Map<Integer, List<T>> bySpec = Maps.newHashMap();
    for (T file : files) {
      bySpec.computeIfAbsent(file.specId(), k -> Lists.newArrayList()).add(file);
    }
    return bySpec;
  }

  private static Map<Integer, List<DataFile>> groupBySpec(List<DataFile> files) {
    return groupBySpecGeneric(files);
  }

  private static Map<Integer, List<DeleteFile>> groupDeletesBySpec(List<DeleteFile> files) {
    return groupBySpecGeneric(files);
  }

  private static ManifestFile writeDataManifest(
      TableOperations ops,
      int formatVersion,
      PartitionSpec spec,
      List<DataFile> dataFiles,
      long snapshotId,
      String commitUUID,
      AtomicInteger manifestCount) {
    EncryptedOutputFile outputFile = newManifestOutputFile(ops, commitUUID, manifestCount);
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(formatVersion, spec, outputFile, snapshotId);

    try {
      for (DataFile file : dataFiles) {
        writer.add(file);
      }
    } catch (RuntimeException e) {
      closeQuietly(writer);
      throw e;
    }

    return closeAndGetManifest(writer, "data manifest");
  }

  private static ManifestFile writeDataManifestWithDeleted(
      TableOperations ops,
      int formatVersion,
      PartitionSpec spec,
      List<DataFile> dataFiles,
      long snapshotId,
      String commitUUID,
      AtomicInteger manifestCount) {
    EncryptedOutputFile outputFile = newManifestOutputFile(ops, commitUUID, manifestCount);
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(formatVersion, spec, outputFile, snapshotId);

    try {
      for (DataFile file : dataFiles) {
        Long dataSeqNum = file.dataSequenceNumber();
        Long fileSeqNum = file.fileSequenceNumber();
        writer.delete(file, dataSeqNum != null ? dataSeqNum : 0L, fileSeqNum);
      }
    } catch (RuntimeException e) {
      closeQuietly(writer);
      throw e;
    }

    return closeAndGetManifest(writer, "data manifest with deleted files");
  }

  private static ManifestFile writeDeleteManifest(
      TableOperations ops,
      int formatVersion,
      PartitionSpec spec,
      List<DeleteFile> deleteFiles,
      long snapshotId,
      String commitUUID,
      AtomicInteger manifestCount) {
    EncryptedOutputFile outputFile = newManifestOutputFile(ops, commitUUID, manifestCount);
    ManifestWriter<DeleteFile> writer =
        ManifestFiles.writeDeleteManifest(formatVersion, spec, outputFile, snapshotId);

    try {
      for (DeleteFile file : deleteFiles) {
        writer.add(file);
      }
    } catch (RuntimeException e) {
      closeQuietly(writer);
      throw e;
    }

    return closeAndGetManifest(writer, "delete manifest");
  }

  private static ManifestFile writeDeleteManifestWithDeleted(
      TableOperations ops,
      int formatVersion,
      PartitionSpec spec,
      List<DeleteFile> deleteFiles,
      long snapshotId,
      String commitUUID,
      AtomicInteger manifestCount) {
    EncryptedOutputFile outputFile = newManifestOutputFile(ops, commitUUID, manifestCount);
    ManifestWriter<DeleteFile> writer =
        ManifestFiles.writeDeleteManifest(formatVersion, spec, outputFile, snapshotId);

    try {
      for (DeleteFile file : deleteFiles) {
        Long dataSeqNum = file.dataSequenceNumber();
        Long fileSeqNum = file.fileSequenceNumber();
        writer.delete(file, dataSeqNum != null ? dataSeqNum : 0L, fileSeqNum);
      }
    } catch (RuntimeException e) {
      closeQuietly(writer);
      throw e;
    }

    return closeAndGetManifest(writer, "delete manifest with removed files");
  }

  private static <F extends ContentFile<F>> ManifestFile closeAndGetManifest(
      ManifestWriter<F> writer, String description) {
    try {
      writer.close();
      return writer.toManifestFile();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close " + description + " writer", e);
    }
  }

  private static void closeQuietly(ManifestWriter<?> writer) {
    try {
      writer.close();
    } catch (IOException suppressed) {
      LOG.warn("Failed to close manifest writer during error handling", suppressed);
    }
  }

  private static EncryptedOutputFile newManifestOutputFile(
      TableOperations ops, String commitUUID, AtomicInteger manifestCount) {
    String manifestFileLocation =
        ops.metadataFileLocation(
            FileFormat.AVRO.addExtension(commitUUID + "-m" + manifestCount.getAndIncrement()));
    return EncryptingFileIO.combine(ops.io(), ops.encryption())
        .newEncryptingOutputFile(manifestFileLocation);
  }

  private static OutputFile manifestListPath(
      TableOperations ops, long snapshotId, int attempt, String commitUUID) {
    return ops.io()
        .newOutputFile(
            ops.metadataFileLocation(
                FileFormat.AVRO.addExtension(
                    String.format(Locale.ROOT, "snap-%d-%d-%s", snapshotId, attempt, commitUUID))));
  }

  private static Map<String, String> buildSummary(
      MetadataUpdate.ProduceSnapshotUpdate update, Snapshot parentSnapshot) {
    SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();

    // Add user-provided summary properties
    if (update.summary() != null) {
      update.summary().forEach(summaryBuilder::set);
    }

    // Calculate file statistics
    int addedDataFiles = update.addDataFiles().size();
    int deletedDataFiles = update.removeDataFiles().size();
    int addedDeleteFiles = update.addDeleteFiles().size();
    int removedDeleteFiles = update.removeDeleteFiles().size();

    long addedRecords = 0;
    long deletedRecords = 0;
    long addedFileSize = 0;
    long removedFileSize = 0;

    for (DataFile file : update.addDataFiles()) {
      addedRecords += file.recordCount();
      addedFileSize += file.fileSizeInBytes();
    }

    for (DataFile file : update.removeDataFiles()) {
      deletedRecords += file.recordCount();
      removedFileSize += file.fileSizeInBytes();
    }

    for (DeleteFile file : update.addDeleteFiles()) {
      addedFileSize += file.fileSizeInBytes();
    }

    for (DeleteFile file : update.removeDeleteFiles()) {
      removedFileSize += file.fileSizeInBytes();
    }

    // Set summary properties
    if (addedDataFiles > 0) {
      summaryBuilder.set(SnapshotSummary.ADDED_FILES_PROP, String.valueOf(addedDataFiles));
      summaryBuilder.set(SnapshotSummary.ADDED_RECORDS_PROP, String.valueOf(addedRecords));
      summaryBuilder.set(SnapshotSummary.ADDED_FILE_SIZE_PROP, String.valueOf(addedFileSize));
    }

    if (deletedDataFiles > 0) {
      summaryBuilder.set(SnapshotSummary.DELETED_FILES_PROP, String.valueOf(deletedDataFiles));
      summaryBuilder.set(SnapshotSummary.DELETED_RECORDS_PROP, String.valueOf(deletedRecords));
      summaryBuilder.set(SnapshotSummary.REMOVED_FILE_SIZE_PROP, String.valueOf(removedFileSize));
    }

    if (addedDeleteFiles > 0) {
      summaryBuilder.set(
          SnapshotSummary.ADDED_DELETE_FILES_PROP, String.valueOf(addedDeleteFiles));
      // Count position vs equality deletes
      long posDeletes = 0;
      long eqDeletes = 0;
      for (DeleteFile file : update.addDeleteFiles()) {
        if (file.content() == FileContent.POSITION_DELETES) {
          posDeletes += file.recordCount();
        } else {
          eqDeletes += file.recordCount();
        }
      }
      if (posDeletes > 0) {
        summaryBuilder.set(SnapshotSummary.ADDED_POS_DELETES_PROP, String.valueOf(posDeletes));
      }
      if (eqDeletes > 0) {
        summaryBuilder.set(SnapshotSummary.ADDED_EQ_DELETES_PROP, String.valueOf(eqDeletes));
      }
    }

    if (removedDeleteFiles > 0) {
      summaryBuilder.set(
          SnapshotSummary.REMOVED_DELETE_FILES_PROP, String.valueOf(removedDeleteFiles));
    }

    // Build with totals from previous snapshot
    Map<String, String> summary = summaryBuilder.build();

    // Compute totals based on previous snapshot
    return computeTotals(summary, parentSnapshot);
  }

  private static Map<String, String> computeTotals(
      Map<String, String> summary, Snapshot parentSnapshot) {
    Map<String, String> previousSummary;
    if (parentSnapshot != null && parentSnapshot.summary() != null) {
      previousSummary = parentSnapshot.summary();
    } else {
      previousSummary =
          ImmutableMap.of(
              SnapshotSummary.TOTAL_RECORDS_PROP, "0",
              SnapshotSummary.TOTAL_FILE_SIZE_PROP, "0",
              SnapshotSummary.TOTAL_DATA_FILES_PROP, "0",
              SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0",
              SnapshotSummary.TOTAL_POS_DELETES_PROP, "0",
              SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0");
    }

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(summary);

    updateTotal(
        builder,
        previousSummary,
        SnapshotSummary.TOTAL_RECORDS_PROP,
        summary,
        SnapshotSummary.ADDED_RECORDS_PROP,
        SnapshotSummary.DELETED_RECORDS_PROP);
    updateTotal(
        builder,
        previousSummary,
        SnapshotSummary.TOTAL_FILE_SIZE_PROP,
        summary,
        SnapshotSummary.ADDED_FILE_SIZE_PROP,
        SnapshotSummary.REMOVED_FILE_SIZE_PROP);
    updateTotal(
        builder,
        previousSummary,
        SnapshotSummary.TOTAL_DATA_FILES_PROP,
        summary,
        SnapshotSummary.ADDED_FILES_PROP,
        SnapshotSummary.DELETED_FILES_PROP);
    updateTotal(
        builder,
        previousSummary,
        SnapshotSummary.TOTAL_DELETE_FILES_PROP,
        summary,
        SnapshotSummary.ADDED_DELETE_FILES_PROP,
        SnapshotSummary.REMOVED_DELETE_FILES_PROP);
    updateTotal(
        builder,
        previousSummary,
        SnapshotSummary.TOTAL_POS_DELETES_PROP,
        summary,
        SnapshotSummary.ADDED_POS_DELETES_PROP,
        SnapshotSummary.REMOVED_POS_DELETES_PROP);
    updateTotal(
        builder,
        previousSummary,
        SnapshotSummary.TOTAL_EQ_DELETES_PROP,
        summary,
        SnapshotSummary.ADDED_EQ_DELETES_PROP,
        SnapshotSummary.REMOVED_EQ_DELETES_PROP);

    return builder.buildKeepingLast();
  }

  private static void updateTotal(
      ImmutableMap.Builder<String, String> builder,
      Map<String, String> previousSummary,
      String totalProperty,
      Map<String, String> currentSummary,
      String addedProperty,
      String deletedProperty) {
    String totalStr = previousSummary.get(totalProperty);
    if (totalStr != null) {
      try {
        long newTotal = Long.parseLong(totalStr);

        String addedStr = currentSummary.get(addedProperty);
        if (newTotal >= 0 && addedStr != null) {
          newTotal += Long.parseLong(addedStr);
        }

        String deletedStr = currentSummary.get(deletedProperty);
        if (newTotal >= 0 && deletedStr != null) {
          newTotal -= Long.parseLong(deletedStr);
        }

        if (newTotal >= 0) {
          builder.put(totalProperty, String.valueOf(newTotal));
        }
      } catch (NumberFormatException e) {
        LOG.warn(
            "Failed to parse snapshot summary total for '{}', skipping total calculation",
            totalProperty,
            e);
      }
    }
  }

  // ==================== Validation Methods ====================

  private static void applyValidation(
      TableOperations ops,
      TableMetadata base,
      Snapshot parent,
      Long baseSnapshotId,
      CommitValidation validation) {
    if (validation instanceof CommitValidation.NotAllowedAddedDataFiles) {
      validateNoAddedDataFiles(
          ops, base, parent, baseSnapshotId, (CommitValidation.NotAllowedAddedDataFiles) validation);
    } else if (validation instanceof CommitValidation.NotAllowedAddedDeleteFiles) {
      validateNoAddedDeleteFiles(
          ops,
          base,
          parent,
          baseSnapshotId,
          (CommitValidation.NotAllowedAddedDeleteFiles) validation);
    } else if (validation instanceof CommitValidation.RequiredDataFiles) {
      validateRequiredDataFiles(
          ops, base, parent, baseSnapshotId, (CommitValidation.RequiredDataFiles) validation);
    } else if (validation instanceof CommitValidation.RequiredDeleteFiles) {
      validateRequiredDeleteFiles(
          ops, base, parent, baseSnapshotId, (CommitValidation.RequiredDeleteFiles) validation);
    } else if (validation instanceof CommitValidation.NotAllowedNewDeletesForDataFiles) {
      validateNoNewDeletesForDataFiles(
          ops,
          base,
          parent,
          baseSnapshotId,
          (CommitValidation.NotAllowedNewDeletesForDataFiles) validation);
    }
  }

  /**
   * Validates that no data files matching the filter have been added since the base snapshot.
   */
  private static void validateNoAddedDataFiles(
      TableOperations ops,
      TableMetadata base,
      Snapshot parent,
      Long baseSnapshotId,
      CommitValidation.NotAllowedAddedDataFiles validation) {
    if (parent == null) {
      return;
    }

    Expression filter = validation.filter();
    Pair<List<ManifestFile>, Set<Long>> history =
        validationHistory(ops, base, baseSnapshotId, VALIDATE_ADDED_FILES_OPERATIONS, ManifestContent.DATA, parent);
    List<ManifestFile> manifests = history.first();
    Set<Long> newSnapshots = history.second();

    ManifestGroup manifestGroup =
        new ManifestGroup(ops.io(), manifests, ImmutableList.of())
            .caseSensitive(true)
            .filterManifestEntries(entry -> newSnapshots.contains(entry.snapshotId()))
            .specsById(base.specsById())
            .ignoreDeleted()
            .ignoreExisting();

    if (filter != null) {
      manifestGroup = manifestGroup.filterData(filter);
    }

    try (CloseableIterable<ManifestEntry<DataFile>> entries = manifestGroup.entries()) {
      CloseableIterator<ManifestEntry<DataFile>> conflicts = entries.iterator();
      if (conflicts.hasNext()) {
        throw new ValidationException(
            "Found conflicting files that can contain records matching %s: %s",
            filter,
            Iterators.toString(
                Iterators.transform(conflicts, entry -> entry.file().location().toString())));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to validate no appends matching %s", filter), e);
    }
  }

  /**
   * Validates that no delete files matching the filter have been added since the base snapshot.
   */
  private static void validateNoAddedDeleteFiles(
      TableOperations ops,
      TableMetadata base,
      Snapshot parent,
      Long baseSnapshotId,
      CommitValidation.NotAllowedAddedDeleteFiles validation) {
    if (parent == null || base.formatVersion() < 2) {
      return;
    }

    Expression filter = validation.filter();
    Pair<List<ManifestFile>, Set<Long>> history =
        validationHistory(
            ops, base, baseSnapshotId, VALIDATE_ADDED_DELETE_FILES_OPERATIONS, ManifestContent.DELETES, parent);
    List<ManifestFile> deleteManifests = history.first();

    long startingSequenceNumber = startingSequenceNumber(base, baseSnapshotId);

    DeleteFileIndex.Builder builder =
        DeleteFileIndex.builderFor(ops.io(), deleteManifests)
            .afterSequenceNumber(startingSequenceNumber)
            .caseSensitive(true)
            .specsById(base.specsById());

    if (filter != null) {
      builder.filterData(filter);
    }

    DeleteFileIndex deletes = builder.build();
    ValidationException.check(
        deletes.isEmpty(),
        "Found new conflicting delete files that can apply to records matching %s: %s",
        filter,
        deletes.referencedDeleteFiles());
  }

  /**
   * Validates that the required data files still exist (haven't been removed by concurrent operations).
   */
  private static void validateRequiredDataFiles(
      TableOperations ops,
      TableMetadata base,
      Snapshot parent,
      Long baseSnapshotId,
      CommitValidation.RequiredDataFiles validation) {
    if (parent == null) {
      return;
    }

    List<String> filePaths = validation.filePaths();
    if (filePaths == null || filePaths.isEmpty()) {
      return;
    }

    Set<String> requiredFiles = Sets.newHashSet(filePaths);
    Set<String> matchingOperations = validation.allowedRemoveOperations();

    Pair<List<ManifestFile>, Set<Long>> history =
        validationHistory(ops, base, baseSnapshotId, matchingOperations, ManifestContent.DATA, parent);
    List<ManifestFile> manifests = history.first();
    Set<Long> newSnapshots = history.second();

    ManifestGroup matchingDeletesGroup =
        new ManifestGroup(ops.io(), manifests, ImmutableList.of())
            .filterManifestEntries(
                entry ->
                    entry.status() != ManifestEntry.Status.ADDED
                        && newSnapshots.contains(entry.snapshotId())
                        && requiredFiles.contains(entry.file().location().toString()))
            .specsById(base.specsById())
            .ignoreExisting();

    Expression filter = validation.filter();
    if (filter != null) {
      matchingDeletesGroup.filterData(filter);
    }

    try (CloseableIterator<ManifestEntry<DataFile>> deletes =
        matchingDeletesGroup.entries().iterator()) {
      if (deletes.hasNext()) {
        throw new ValidationException(
            "Cannot commit, missing data files: %s",
            Iterators.toString(
                Iterators.transform(deletes, entry -> entry.file().location().toString())));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to validate required files exist", e);
    }
  }

  /**
   * Validates that the required delete files still exist (haven't been removed by concurrent
   * operations).
   */
  private static void validateRequiredDeleteFiles(
      TableOperations ops,
      TableMetadata base,
      Snapshot parent,
      Long baseSnapshotId,
      CommitValidation.RequiredDeleteFiles validation) {
    if (parent == null || base.formatVersion() < 2) {
      return;
    }

    List<String> filePaths = validation.filePaths();
    if (filePaths == null || filePaths.isEmpty()) {
      return;
    }

    Set<String> requiredFiles = Sets.newHashSet(filePaths);
    // Delete files can be removed by compaction operations (REPLACE)
    Set<String> matchingOperations = Sets.newHashSet(DataOperations.REPLACE);

    Pair<List<ManifestFile>, Set<Long>> history =
        validationHistory(
            ops, base, baseSnapshotId, matchingOperations, ManifestContent.DELETES, parent);
    List<ManifestFile> manifests = history.first();
    Set<Long> newSnapshots = history.second();

    // Check if any required delete files were removed in the new snapshots
    for (ManifestFile manifest : manifests) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, ops.io(), base.specsById())) {
        for (ManifestEntry<DeleteFile> entry : reader.entries()) {
          // Look for entries that were deleted (removed) in new snapshots
          if (entry.status() == ManifestEntry.Status.DELETED
              && newSnapshots.contains(entry.snapshotId())
              && requiredFiles.contains(entry.file().location().toString())) {
            throw new ValidationException(
                "Cannot commit, missing delete file: %s", entry.file().location());
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to validate required delete files exist", e);
      }
    }
  }

  /**
   * Validates that no new delete files have been applied to specific data files.
   *
   * <p>Note: This implementation is conservative. It checks whether any new delete files exist that
   * could potentially apply to the specified data files based on partition matching, but the
   * partition-level filtering is done via the DeleteFileIndex. If the filter is broad, it may
   * reject commits where the new deletes don't actually affect the specific data files. This is
   * safe (no data corruption) but may cause unnecessary conflicts.
   */
  private static void validateNoNewDeletesForDataFiles(
      TableOperations ops,
      TableMetadata base,
      Snapshot parent,
      Long baseSnapshotId,
      CommitValidation.NotAllowedNewDeletesForDataFiles validation) {
    if (parent == null || base.formatVersion() < 2) {
      return;
    }

    Expression filter = validation.filter();
    Pair<List<ManifestFile>, Set<Long>> history =
        validationHistory(
            ops,
            base,
            baseSnapshotId,
            VALIDATE_ADDED_DELETE_FILES_OPERATIONS,
            ManifestContent.DELETES,
            parent);
    List<ManifestFile> deleteManifests = history.first();

    long startingSequenceNumber = startingSequenceNumber(base, baseSnapshotId);

    DeleteFileIndex.Builder builder =
        DeleteFileIndex.builderFor(ops.io(), deleteManifests)
            .afterSequenceNumber(startingSequenceNumber)
            .caseSensitive(true)
            .specsById(base.specsById());

    if (filter != null) {
      builder.filterData(filter);
    }

    DeleteFileIndex deletes = builder.build();

    // Check if new delete files exist that could apply to records matching the filter.
    // The DeleteFileIndex is already filtered by the partition filter, so checking isEmpty()
    // tells us if any new deletes exist in the filtered partition space.
    if (!deletes.isEmpty()) {
      List<String> dataFilePaths = validation.filePaths();
      String pathsInfo = dataFilePaths != null ? String.join(", ", dataFilePaths) : "unspecified";
      throw new ValidationException(
          "Cannot commit, found new delete files that may apply to data files matching filter %s: %s",
          filter, pathsInfo);
    }
  }

  /**
   * Returns newly added manifests and snapshot IDs between the starting and parent snapshots.
   */
  private static Pair<List<ManifestFile>, Set<Long>> validationHistory(
      TableOperations ops,
      TableMetadata base,
      Long startingSnapshotId,
      Set<String> matchingOperations,
      ManifestContent content,
      Snapshot parent) {
    List<ManifestFile> manifests = Lists.newArrayList();
    Set<Long> newSnapshots = Sets.newHashSet();

    Snapshot lastSnapshot = null;
    Iterable<Snapshot> snapshots =
        SnapshotUtil.ancestorsBetween(parent.snapshotId(), startingSnapshotId, base::snapshot);

    for (Snapshot currentSnapshot : snapshots) {
      lastSnapshot = currentSnapshot;

      if (matchingOperations.contains(currentSnapshot.operation())) {
        newSnapshots.add(currentSnapshot.snapshotId());
        if (content == ManifestContent.DATA) {
          for (ManifestFile manifest : currentSnapshot.dataManifests(ops.io())) {
            if (manifest.snapshotId() == currentSnapshot.snapshotId()) {
              manifests.add(manifest);
            }
          }
        } else {
          for (ManifestFile manifest : currentSnapshot.deleteManifests(ops.io())) {
            if (manifest.snapshotId() == currentSnapshot.snapshotId()) {
              manifests.add(manifest);
            }
          }
        }
      }
    }

    ValidationException.check(
        lastSnapshot == null
            || java.util.Objects.equals(lastSnapshot.parentId(), startingSnapshotId),
        "Cannot determine history between starting snapshot %s and the last known ancestor %s",
        startingSnapshotId,
        lastSnapshot != null ? lastSnapshot.snapshotId() : null);

    return Pair.of(manifests, newSnapshots);
  }

  private static long startingSequenceNumber(TableMetadata base, Long startingSnapshotId) {
    if (startingSnapshotId != null && base.snapshot(startingSnapshotId) != null) {
      Snapshot startingSnapshot = base.snapshot(startingSnapshotId);
      return startingSnapshot.sequenceNumber();
    }
    return TableMetadata.INITIAL_SEQUENCE_NUMBER;
  }
}
