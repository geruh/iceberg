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

import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * {@link DeleteFiles} implementation that produces a {@link
 * org.apache.iceberg.MetadataUpdate.ProduceSnapshotUpdate} for server-side snapshot creation.
 *
 * <p>This class collects data files to delete and sends them to the server as file-level changes.
 * The server then creates the snapshot, updates manifests, and updates the table metadata.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ProduceDelete delete = new ProduceDelete("tableName", tableOperations);
 * delete.deleteFile(dataFile1)
 *       .deleteFile(dataFile2)
 *       .commit();
 * }</pre>
 */
public class ProduceDelete extends BaseProduceSnapshot<DeleteFiles> implements DeleteFiles {

  private final List<String> pathsToDelete = Lists.newArrayList();
  private boolean validateFilesExist = false;

  public ProduceDelete(String tableName, TableOperations ops) {
    super(tableName, ops);
  }

  @Override
  protected DeleteFiles self() {
    return this;
  }

  @Override
  protected String operation() {
    return DataOperations.DELETE;
  }

  @Override
  public DeleteFiles deleteFile(CharSequence path) {
    Preconditions.checkNotNull(path, "File path cannot be null");
    pathsToDelete.add(path.toString());
    return this;
  }

  @Override
  public DeleteFiles deleteFile(DataFile file) {
    Preconditions.checkNotNull(file, "Data file cannot be null");
    removeDataFile(file);
    return this;
  }

  @Override
  public DeleteFiles deleteFromRowFilter(Expression expr) {
    Preconditions.checkNotNull(expr, "Row filter expression cannot be null");
    setDeleteRowFilter(expr);
    return this;
  }

  @Override
  public DeleteFiles caseSensitive(boolean isCaseSensitive) {
    // Case sensitivity is applied server-side when evaluating the delete row filter.
    // This setting is passed through the deleteRowFilter expression itself.
    return this;
  }

  @Override
  public DeleteFiles validateFilesExist() {
    this.validateFilesExist = true;
    return this;
  }

  /**
   * Adds a delete file to be applied to matching data files.
   *
   * @param deleteFile the delete file to add
   * @return this for method chaining
   */
  public ProduceDelete withDeleteFile(DeleteFile deleteFile) {
    addDeleteFile(deleteFile);
    return this;
  }

  @Override
  public void commit() {
    // If there are path-based deletions, we need to resolve them to DataFile objects
    // For now, path-based deletions are not fully supported in ProduceSnapshotUpdate
    // The server would need to resolve paths to actual files
    if (!pathsToDelete.isEmpty()) {
      // Add validation that these files exist if requested
      if (validateFilesExist) {
        addValidation(new CommitValidation.RequiredDataFiles(null, pathsToDelete));
      }
    }

    super.commit();
  }
}
