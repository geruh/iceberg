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

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/**
 * Represents a validation that must pass at commit time for a ProduceSnapshotUpdate operation.
 *
 * <p>These validations are evaluated on the server side to detect concurrent modification
 * conflicts before committing a new snapshot.
 */
public interface CommitValidation extends Serializable {

  /**
   * The type of validation.
   *
   * @return the validation type string
   */
  String type();

  /**
   * Validates this condition against the given table metadata.
   *
   * @param base the current table metadata
   * @param parent the parent snapshot for the new snapshot being committed
   * @param baseSnapshotId the snapshot ID from which changes were computed
   */
  void validate(TableMetadata base, Snapshot parent, Long baseSnapshotId);

  /** Validation that no data files matching a filter have been added since the base snapshot. */
  class NotAllowedAddedDataFiles implements CommitValidation {
    public static final String TYPE = "not-allowed-added-data-files";

    private final Expression filter;

    public NotAllowedAddedDataFiles(Expression filter) {
      this.filter = Preconditions.checkNotNull(filter, "Filter cannot be null");
    }

    @Override
    public String type() {
      return TYPE;
    }

    public Expression filter() {
      return filter;
    }

    @Override
    public void validate(TableMetadata base, Snapshot parent, Long baseSnapshotId) {
      // Validation is performed using MergingSnapshotProducer.validateAddedDataFiles
      // This is a marker class - actual validation is done by ProduceSnapshotUpdateHandler
    }
  }

  /** Validation that no delete files matching a filter have been added since the base snapshot. */
  class NotAllowedAddedDeleteFiles implements CommitValidation {
    public static final String TYPE = "not-allowed-added-delete-files";

    private final Expression filter;

    public NotAllowedAddedDeleteFiles(Expression filter) {
      this.filter = Preconditions.checkNotNull(filter, "Filter cannot be null");
    }

    @Override
    public String type() {
      return TYPE;
    }

    public Expression filter() {
      return filter;
    }

    @Override
    public void validate(TableMetadata base, Snapshot parent, Long baseSnapshotId) {
      // Validation is performed using MergingSnapshotProducer.validateNoNewDeleteFiles
      // This is a marker class - actual validation is done by ProduceSnapshotUpdateHandler
    }
  }

  /** Validation that required data files still exist at commit time. */
  class RequiredDataFiles implements CommitValidation {
    public static final String TYPE = "required-data-files";

    private static final Set<String> DEFAULT_ALLOWED_REMOVE_OPERATIONS =
        ImmutableSet.of(DataOperations.DELETE, DataOperations.OVERWRITE, DataOperations.REPLACE);

    private final Expression filter;
    private final List<String> filePaths;
    private final Set<String> allowedRemoveOperations;

    public RequiredDataFiles(Expression filter, List<String> filePaths) {
      this(filter, filePaths, DEFAULT_ALLOWED_REMOVE_OPERATIONS);
    }

    public RequiredDataFiles(
        Expression filter, List<String> filePaths, Set<String> allowedRemoveOperations) {
      Preconditions.checkArgument(
          filter != null || (filePaths != null && !filePaths.isEmpty()),
          "RequiredDataFiles validation must have either a filter or file paths");
      this.filter = filter;
      this.filePaths = filePaths;
      this.allowedRemoveOperations =
          allowedRemoveOperations != null
              ? ImmutableSet.copyOf(allowedRemoveOperations)
              : DEFAULT_ALLOWED_REMOVE_OPERATIONS;
    }

    @Override
    public String type() {
      return TYPE;
    }

    public Expression filter() {
      return filter;
    }

    public List<String> filePaths() {
      return filePaths;
    }

    public Set<String> allowedRemoveOperations() {
      return allowedRemoveOperations;
    }

    @Override
    public void validate(TableMetadata base, Snapshot parent, Long baseSnapshotId) {
      // Validation is performed using MergingSnapshotProducer.validateDataFilesExist
      // This is a marker class - actual validation is done by ProduceSnapshotUpdateHandler
    }
  }

  /** Validation that required delete files still exist at commit time. */
  class RequiredDeleteFiles implements CommitValidation {
    public static final String TYPE = "required-delete-files";

    private final Expression filter;
    private final List<String> filePaths;

    public RequiredDeleteFiles(Expression filter, List<String> filePaths) {
      Preconditions.checkArgument(
          filter != null || (filePaths != null && !filePaths.isEmpty()),
          "RequiredDeleteFiles validation must have either a filter or file paths");
      this.filter = filter;
      this.filePaths = filePaths;
    }

    @Override
    public String type() {
      return TYPE;
    }

    public Expression filter() {
      return filter;
    }

    public List<String> filePaths() {
      return filePaths;
    }

    @Override
    public void validate(TableMetadata base, Snapshot parent, Long baseSnapshotId) {
      // Validation is performed similarly to RequiredDataFiles but for delete files
      // This is a marker class - actual validation is done by ProduceSnapshotUpdateHandler
    }
  }

  /**
   * Validation that no new delete files have been applied to specific data files since the base
   * snapshot.
   */
  class NotAllowedNewDeletesForDataFiles implements CommitValidation {
    public static final String TYPE = "not-allowed-new-deletes-for-data-files";

    private final List<String> filePaths;
    private final Expression filter;

    public NotAllowedNewDeletesForDataFiles(List<String> filePaths, Expression filter) {
      Preconditions.checkArgument(
          filter != null || (filePaths != null && !filePaths.isEmpty()),
          "NotAllowedNewDeletesForDataFiles validation must have either a filter or file paths");
      this.filePaths = filePaths;
      this.filter = filter;
    }

    @Override
    public String type() {
      return TYPE;
    }

    public List<String> filePaths() {
      return filePaths;
    }

    public Expression filter() {
      return filter;
    }

    @Override
    public void validate(TableMetadata base, Snapshot parent, Long baseSnapshotId) {
      // Validation is performed using MergingSnapshotProducer.validateNoNewDeletesForDataFiles
      // This is a marker class - actual validation is done by ProduceSnapshotUpdateHandler
    }
  }
}
