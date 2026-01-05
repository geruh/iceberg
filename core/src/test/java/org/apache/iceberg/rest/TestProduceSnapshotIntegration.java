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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.MetadataUpdateParser;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for ProduceSnapshotUpdate serialization and deserialization through the REST
 * API request/response cycle.
 */
public class TestProduceSnapshotIntegration {

  private static final PartitionSpec SPEC = TestBase.SPEC;
  private static final Map<Integer, PartitionSpec> SPECS_BY_ID =
      ImmutableMap.of(SPEC.specId(), SPEC);

  // ============ Full Round-Trip Tests ============

  @Test
  public void testProduceSnapshotUpdateFullRoundTrip() {
    // Create a complete ProduceSnapshotUpdate with all fields
    MetadataUpdate.ProduceSnapshotUpdate original =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.OVERWRITE,
            ImmutableList.of(TestBase.FILE_A, TestBase.FILE_B),
            ImmutableList.of(TestBase.FILE_A_DELETES),
            ImmutableList.of(TestBase.FILE_C),
            Collections.emptyList(),
            Expressions.equal("date", "2024-01-01"),
            true,
            "feature-branch",
            ImmutableMap.of("commit-id", "abc123", "author", "test-user"),
            987654321L,
            ImmutableList.of(
                new CommitValidation.NotAllowedAddedDataFiles(
                    Expressions.equal("partition_key", "value")),
                new CommitValidation.NotAllowedAddedDeleteFiles(Expressions.alwaysTrue())));

    // Serialize to JSON
    String json = MetadataUpdateParser.toJson(original, SPECS_BY_ID);

    // Verify all fields are present in JSON
    assertThat(json).contains("\"action\":\"produce-snapshot\"");
    assertThat(json).contains("\"snapshot-action\":\"overwrite\"");
    assertThat(json).contains("\"add-data-files\"");
    assertThat(json).contains("\"add-delete-files\"");
    assertThat(json).contains("\"remove-data-files\"");
    assertThat(json).contains("\"delete-row-filter\"");
    assertThat(json).contains("\"stage-only\":true");
    assertThat(json).contains("\"branch\":\"feature-branch\"");
    assertThat(json).contains("\"summary\"");
    assertThat(json).contains("\"commit-id\":\"abc123\"");
    assertThat(json).contains("\"base-snapshot-id\":987654321");
    assertThat(json).contains("\"commit-validations\"");
    assertThat(json).contains("\"not-allowed-added-data-files\"");
    assertThat(json).contains("\"not-allowed-added-delete-files\"");

    // Parse back from JSON
    MetadataUpdate parsed = MetadataUpdateParser.fromJson(json);
    assertThat(parsed).isInstanceOf(MetadataUpdate.ProduceSnapshotUpdate.class);

    MetadataUpdate.ProduceSnapshotUpdate result = (MetadataUpdate.ProduceSnapshotUpdate) parsed;
    assertThat(result.isUnresolved()).isTrue();

    // Resolve using partition specs
    MetadataUpdate.ProduceSnapshotUpdate resolved = result.resolve(SPECS_BY_ID);
    assertThat(resolved.isUnresolved()).isFalse();

    // Verify all fields were preserved
    assertThat(resolved.action()).isEqualTo(DataOperations.OVERWRITE);
    assertThat(resolved.addDataFiles()).hasSize(2);
    assertThat(resolved.addDeleteFiles()).hasSize(1);
    assertThat(resolved.removeDataFiles()).hasSize(1);
    assertThat(resolved.removeDeleteFiles()).isEmpty();
    assertThat(resolved.deleteRowFilter()).isNotNull();
    assertThat(resolved.stageOnly()).isTrue();
    assertThat(resolved.branch()).isEqualTo("feature-branch");
    assertThat(resolved.summary()).containsEntry("commit-id", "abc123");
    assertThat(resolved.summary()).containsEntry("author", "test-user");
    assertThat(resolved.baseSnapshotId()).isEqualTo(987654321L);
    assertThat(resolved.validations()).hasSize(2);
    assertThat(resolved.validations().get(0))
        .isInstanceOf(CommitValidation.NotAllowedAddedDataFiles.class);
    assertThat(resolved.validations().get(1))
        .isInstanceOf(CommitValidation.NotAllowedAddedDeleteFiles.class);
  }

  @Test
  public void testProduceSnapshotUpdateMinimalRoundTrip() {
    // Create a minimal ProduceSnapshotUpdate
    MetadataUpdate.ProduceSnapshotUpdate original =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.APPEND,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            false,
            null,
            null,
            null,
            null);

    String json = MetadataUpdateParser.toJson(original, SPECS_BY_ID);

    // Should have minimal fields
    assertThat(json).contains("\"snapshot-action\":\"append\"");
    assertThat(json).contains("\"stage-only\":false");
    assertThat(json).doesNotContain("\"branch\"");
    assertThat(json).doesNotContain("\"summary\"");
    assertThat(json).doesNotContain("\"base-snapshot-id\"");
    assertThat(json).doesNotContain("\"commit-validations\"");

    MetadataUpdate parsed = MetadataUpdateParser.fromJson(json);
    MetadataUpdate.ProduceSnapshotUpdate result = (MetadataUpdate.ProduceSnapshotUpdate) parsed;

    assertThat(result.action()).isEqualTo(DataOperations.APPEND);
    assertThat(result.stageOnly()).isFalse();
    assertThat(result.branch()).isNull();
    assertThat(result.summary()).isEmpty();
    assertThat(result.baseSnapshotId()).isNull();
    assertThat(result.validations()).isEmpty();
  }

  // ============ Validation Round-Trip Tests ============

  @Test
  public void testAllValidationTypesRoundTrip() {
    List<CommitValidation> validations =
        ImmutableList.of(
            new CommitValidation.NotAllowedAddedDataFiles(Expressions.equal("partition", "A")),
            new CommitValidation.NotAllowedAddedDeleteFiles(Expressions.greaterThan("ts", 1000)),
            new CommitValidation.RequiredDataFiles(
                Expressions.lessThan("date", "2024-01-01"),
                ImmutableList.of("/path/to/file1.parquet")),
            new CommitValidation.RequiredDeleteFiles(
                null, ImmutableList.of("/path/to/delete1.parquet")),
            new CommitValidation.NotAllowedNewDeletesForDataFiles(
                ImmutableList.of("/path/to/data.parquet"), Expressions.alwaysTrue()));

    MetadataUpdate.ProduceSnapshotUpdate original =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.OVERWRITE,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            false,
            null,
            null,
            null,
            validations);

    String json = MetadataUpdateParser.toJson(original, SPECS_BY_ID);
    MetadataUpdate.ProduceSnapshotUpdate parsed =
        (MetadataUpdate.ProduceSnapshotUpdate) MetadataUpdateParser.fromJson(json);

    assertThat(parsed.validations()).hasSize(5);

    assertThat(parsed.validations().get(0))
        .isInstanceOf(CommitValidation.NotAllowedAddedDataFiles.class);
    assertThat(((CommitValidation.NotAllowedAddedDataFiles) parsed.validations().get(0)).filter())
        .isNotNull();

    assertThat(parsed.validations().get(1))
        .isInstanceOf(CommitValidation.NotAllowedAddedDeleteFiles.class);

    assertThat(parsed.validations().get(2)).isInstanceOf(CommitValidation.RequiredDataFiles.class);
    CommitValidation.RequiredDataFiles requiredData =
        (CommitValidation.RequiredDataFiles) parsed.validations().get(2);
    assertThat(requiredData.filter()).isNotNull();
    assertThat(requiredData.filePaths()).hasSize(1);

    assertThat(parsed.validations().get(3))
        .isInstanceOf(CommitValidation.RequiredDeleteFiles.class);
    CommitValidation.RequiredDeleteFiles requiredDelete =
        (CommitValidation.RequiredDeleteFiles) parsed.validations().get(3);
    assertThat(requiredDelete.filter()).isNull();
    assertThat(requiredDelete.filePaths()).hasSize(1);

    assertThat(parsed.validations().get(4))
        .isInstanceOf(CommitValidation.NotAllowedNewDeletesForDataFiles.class);
  }

  // ============ Complex Expression Tests ============

  @Test
  public void testComplexFilterExpressionsRoundTrip() {
    Expression complexFilter =
        Expressions.and(
            Expressions.or(Expressions.equal("region", "us-west"), Expressions.equal("region", "us-east")),
            Expressions.and(
                Expressions.greaterThan("timestamp", 1704067200000L),
                Expressions.lessThanOrEqual("timestamp", 1706745599000L)),
            Expressions.notNull("user_id"));

    MetadataUpdate.ProduceSnapshotUpdate original =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.DELETE,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            complexFilter,
            false,
            null,
            null,
            null,
            ImmutableList.of(new CommitValidation.NotAllowedAddedDataFiles(complexFilter)));

    String json = MetadataUpdateParser.toJson(original, SPECS_BY_ID);
    MetadataUpdate.ProduceSnapshotUpdate parsed =
        (MetadataUpdate.ProduceSnapshotUpdate) MetadataUpdateParser.fromJson(json);

    assertThat(parsed.deleteRowFilter()).isNotNull();
    assertThat(parsed.deleteRowFilter().toString()).contains("and");
    assertThat(parsed.deleteRowFilter().toString()).contains("or");

    assertThat(parsed.validations()).hasSize(1);
    CommitValidation.NotAllowedAddedDataFiles validation =
        (CommitValidation.NotAllowedAddedDataFiles) parsed.validations().get(0);
    assertThat(validation.filter().toString()).contains("and");
    assertThat(validation.filter().toString()).contains("or");
  }

  // ============ File Content Tests ============

  @Test
  public void testDataFilePropertiesPreserved() {
    DataFile file = TestBase.FILE_A;

    MetadataUpdate.ProduceSnapshotUpdate original =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.APPEND,
            ImmutableList.of(file),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            false,
            null,
            null,
            null,
            null);

    String json = MetadataUpdateParser.toJson(original, SPECS_BY_ID);
    MetadataUpdate.ProduceSnapshotUpdate parsed =
        (MetadataUpdate.ProduceSnapshotUpdate) MetadataUpdateParser.fromJson(json);
    MetadataUpdate.ProduceSnapshotUpdate resolved = parsed.resolve(SPECS_BY_ID);

    assertThat(resolved.addDataFiles()).hasSize(1);
    DataFile parsedFile = resolved.addDataFiles().get(0);

    assertThat(parsedFile.path().toString()).isEqualTo(file.path().toString());
    assertThat(parsedFile.fileSizeInBytes()).isEqualTo(file.fileSizeInBytes());
    assertThat(parsedFile.recordCount()).isEqualTo(file.recordCount());
    assertThat(parsedFile.specId()).isEqualTo(file.specId());
    assertThat(parsedFile.format()).isEqualTo(file.format());
  }

  @Test
  public void testDeleteFilePropertiesPreserved() {
    DeleteFile file = TestBase.FILE_A_DELETES;

    MetadataUpdate.ProduceSnapshotUpdate original =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.DELETE,
            Collections.emptyList(),
            ImmutableList.of(file),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            false,
            null,
            null,
            null,
            null);

    String json = MetadataUpdateParser.toJson(original, SPECS_BY_ID);
    MetadataUpdate.ProduceSnapshotUpdate parsed =
        (MetadataUpdate.ProduceSnapshotUpdate) MetadataUpdateParser.fromJson(json);
    MetadataUpdate.ProduceSnapshotUpdate resolved = parsed.resolve(SPECS_BY_ID);

    assertThat(resolved.addDeleteFiles()).hasSize(1);
    DeleteFile parsedFile = resolved.addDeleteFiles().get(0);

    assertThat(parsedFile.path().toString()).isEqualTo(file.path().toString());
    assertThat(parsedFile.fileSizeInBytes()).isEqualTo(file.fileSizeInBytes());
    assertThat(parsedFile.recordCount()).isEqualTo(file.recordCount());
    assertThat(parsedFile.specId()).isEqualTo(file.specId());
  }

  // ============ Branch Tests ============

  @Test
  public void testBranchTargeting() {
    String[] branches = {"main", "feature-branch", "test/branch-with-slash", "branch_with_underscore"};

    for (String branch : branches) {
      MetadataUpdate.ProduceSnapshotUpdate original =
          new MetadataUpdate.ProduceSnapshotUpdate(
              DataOperations.APPEND,
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              null,
              false,
              branch,
              null,
              null,
              null);

      String json = MetadataUpdateParser.toJson(original, SPECS_BY_ID);
      MetadataUpdate.ProduceSnapshotUpdate parsed =
          (MetadataUpdate.ProduceSnapshotUpdate) MetadataUpdateParser.fromJson(json);

      assertThat(parsed.branch()).isEqualTo(branch);
    }
  }

  // ============ Summary Properties Tests ============

  @Test
  public void testSummaryPropertiesPreserved() {
    Map<String, String> summary =
        ImmutableMap.of(
            "commit-id", "abc123",
            "author", "test-user",
            "source", "spark-job-xyz",
            "special-chars", "value with spaces and !@#$%");

    MetadataUpdate.ProduceSnapshotUpdate original =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.APPEND,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            false,
            null,
            summary,
            null,
            null);

    String json = MetadataUpdateParser.toJson(original, SPECS_BY_ID);
    MetadataUpdate.ProduceSnapshotUpdate parsed =
        (MetadataUpdate.ProduceSnapshotUpdate) MetadataUpdateParser.fromJson(json);

    assertThat(parsed.summary()).containsAllEntriesOf(summary);
  }

  // ============ Re-Serialization Tests ============

  @Test
  public void testUnresolvedCanBeReSerializedWithoutSpecs() {
    // Parse from JSON (creates unresolved)
    String originalJson =
        "{\"action\":\"produce-snapshot\",\"snapshot-action\":\"append\","
            + "\"stage-only\":false,"
            + "\"add-data-files\":[{\"spec-id\":0,\"content\":\"data\","
            + "\"file-path\":\"/path/to/data.parquet\",\"file-format\":\"parquet\","
            + "\"partition\":[1],\"file-size-in-bytes\":10,\"record-count\":1}]}";

    MetadataUpdate parsed = MetadataUpdateParser.fromJson(originalJson);
    MetadataUpdate.ProduceSnapshotUpdate unresolved = (MetadataUpdate.ProduceSnapshotUpdate) parsed;
    assertThat(unresolved.isUnresolved()).isTrue();

    // Unresolved can be re-serialized without partition specs
    String reserializedJson = MetadataUpdateParser.toJson(unresolved);
    assertThat(reserializedJson).contains("\"add-data-files\"");
    assertThat(reserializedJson).contains("/path/to/data.parquet");
  }

  @Test
  public void testMultipleResolveUnresolvedCycles() {
    MetadataUpdate.ProduceSnapshotUpdate original =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.APPEND,
            ImmutableList.of(TestBase.FILE_A),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            false,
            null,
            null,
            null,
            null);

    // Cycle 1: serialize -> parse -> resolve
    String json1 = MetadataUpdateParser.toJson(original, SPECS_BY_ID);
    MetadataUpdate.ProduceSnapshotUpdate parsed1 =
        (MetadataUpdate.ProduceSnapshotUpdate) MetadataUpdateParser.fromJson(json1);
    MetadataUpdate.ProduceSnapshotUpdate resolved1 = parsed1.resolve(SPECS_BY_ID);

    // Cycle 2: serialize resolved -> parse -> resolve
    String json2 = MetadataUpdateParser.toJson(resolved1, SPECS_BY_ID);
    MetadataUpdate.ProduceSnapshotUpdate parsed2 =
        (MetadataUpdate.ProduceSnapshotUpdate) MetadataUpdateParser.fromJson(json2);
    MetadataUpdate.ProduceSnapshotUpdate resolved2 = parsed2.resolve(SPECS_BY_ID);

    // Results should be equivalent
    assertThat(resolved2.addDataFiles()).hasSize(1);
    assertThat(resolved2.addDataFiles().get(0).path().toString())
        .isEqualTo(resolved1.addDataFiles().get(0).path().toString());
  }
}
