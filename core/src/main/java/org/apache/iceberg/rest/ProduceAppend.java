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

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.TableOperations;

/**
 * {@link AppendFiles} implementation that produces a {@link
 * org.apache.iceberg.MetadataUpdate.ProduceSnapshotUpdate} for server-side snapshot creation.
 *
 * <p>This class collects data files to append and sends them to the server as file-level changes.
 * The server then creates the snapshot, writes manifests, and updates the table metadata.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ProduceAppend append = new ProduceAppend("tableName", tableOperations);
 * append.appendFile(dataFile1)
 *       .appendFile(dataFile2)
 *       .set("commit-id", "abc123")
 *       .commit();
 * }</pre>
 */
public class ProduceAppend extends BaseProduceSnapshot<AppendFiles> implements AppendFiles {

  public ProduceAppend(String tableName, TableOperations ops) {
    super(tableName, ops);
  }

  @Override
  protected AppendFiles self() {
    return this;
  }

  @Override
  protected String operation() {
    return DataOperations.APPEND;
  }

  @Override
  public AppendFiles appendFile(DataFile file) {
    addDataFile(file);
    return this;
  }

  @Override
  public AppendFiles appendManifest(ManifestFile file) {
    // For ProduceSnapshotUpdate, we don't support appending manifest files directly.
    // The server creates manifests from the individual data files.
    // Clients should append individual data files instead.
    throw new UnsupportedOperationException(
        "ProduceAppend does not support appendManifest. "
            + "Use appendFile() to add individual data files. "
            + "The server will create manifest files during commit.");
  }
}
