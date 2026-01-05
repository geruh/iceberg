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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.DeleteFileSet;

/**
 * {@link OverwriteFiles} implementation that produces a {@link
 * org.apache.iceberg.MetadataUpdate.ProduceSnapshotUpdate} for server-side snapshot creation.
 *
 * <p>This class collects data files to add and remove, and sends them to the server as file-level
 * changes. The server then creates the snapshot, writes manifests, and updates the table metadata.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ProduceOverwrite overwrite = new ProduceOverwrite("tableName", tableOperations);
 * overwrite.overwriteByRowFilter(Expressions.equal("date", "2024-01-01"))
 *          .addFile(newDataFile)
 *          .validateNoConflictingData()
 *          .commit();
 * }</pre>
 */
public class ProduceOverwrite extends BaseProduceSnapshot<OverwriteFiles> implements OverwriteFiles {

  private Expression conflictDetectionFilter = null;
  private boolean validateNoConflictingData = false;
  private boolean validateNoConflictingDeletes = false;

  public ProduceOverwrite(String tableName, TableOperations ops) {
    super(tableName, ops);
  }

  @Override
  protected OverwriteFiles self() {
    return this;
  }

  @Override
  protected String operation() {
    // Determine operation based on what files are being added/removed
    boolean hasAdds = !addDataFiles().isEmpty();
    boolean hasDeletes = !removeDataFiles().isEmpty() || deleteRowFilter() != null;

    if (hasDeletes && !hasAdds) {
      return DataOperations.DELETE;
    } else if (hasAdds && !hasDeletes) {
      return DataOperations.APPEND;
    }
    return DataOperations.OVERWRITE;
  }

  @Override
  public OverwriteFiles overwriteByRowFilter(Expression expr) {
    Preconditions.checkNotNull(expr, "Row filter expression cannot be null");
    setDeleteRowFilter(expr);
    return this;
  }

  @Override
  public OverwriteFiles addFile(DataFile file) {
    addDataFile(file);
    return this;
  }

  @Override
  public OverwriteFiles deleteFile(DataFile file) {
    removeDataFile(file);
    return this;
  }

  @Override
  public OverwriteFiles deleteFiles(DataFileSet dataFilesToDelete, DeleteFileSet deleteFilesToDelete) {
    Preconditions.checkNotNull(dataFilesToDelete, "Data files to delete cannot be null");

    for (DataFile dataFile : dataFilesToDelete) {
      removeDataFile(dataFile);
    }

    if (deleteFilesToDelete != null) {
      for (DeleteFile deleteFile : deleteFilesToDelete) {
        removeDeleteFile(deleteFile);
      }
    }

    return this;
  }

  @Override
  public OverwriteFiles validateAddedFilesMatchOverwriteFilter() {
    // Server-side validation will check that added files match the overwrite filter.
    // The overwrite filter is passed through deleteRowFilter.
    return this;
  }

  @Override
  public OverwriteFiles validateFromSnapshot(long snapshotId) {
    setBaseSnapshotId(snapshotId);
    return this;
  }

  @Override
  public OverwriteFiles caseSensitive(boolean isCaseSensitive) {
    // Case sensitivity is applied server-side when evaluating filters.
    return this;
  }

  @Override
  public OverwriteFiles conflictDetectionFilter(Expression filter) {
    Preconditions.checkNotNull(filter, "Conflict detection filter cannot be null");
    this.conflictDetectionFilter = filter;
    return this;
  }

  @Override
  public OverwriteFiles validateNoConflictingData() {
    this.validateNoConflictingData = true;
    return this;
  }

  @Override
  public OverwriteFiles validateNoConflictingDeletes() {
    this.validateNoConflictingDeletes = true;
    return this;
  }

  /**
   * Adds a delete file to be applied to matching data files.
   *
   * @param deleteFile the delete file to add
   * @return this for method chaining
   */
  public ProduceOverwrite withDeleteFile(DeleteFile deleteFile) {
    addDeleteFile(deleteFile);
    return this;
  }

  @Override
  public void commit() {
    // Build validations based on configuration
    Expression validationFilter =
        conflictDetectionFilter != null ? conflictDetectionFilter : Expressions.alwaysTrue();

    if (validateNoConflictingData) {
      addValidation(new CommitValidation.NotAllowedAddedDataFiles(validationFilter));
    }

    if (validateNoConflictingDeletes) {
      addValidation(new CommitValidation.NotAllowedAddedDeleteFiles(validationFilter));
    }

    super.commit();
  }
}
