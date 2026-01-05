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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.CommitValidation;
import org.junit.jupiter.api.Test;

public class TestProduceSnapshotUpdateParser {

  private static final PartitionSpec SPEC = TestBase.SPEC;
  private static final Map<Integer, PartitionSpec> SPECS_BY_ID =
      ImmutableMap.of(SPEC.specId(), SPEC);

  @Test
  public void testProduceSnapshotUpdateRoundTrip() {
    MetadataUpdate.ProduceSnapshotUpdate update =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.APPEND,
            ImmutableList.of(TestBase.FILE_A),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            false,
            "main",
            ImmutableMap.of("key", "value"),
            123456789L,
            null);

    String json = MetadataUpdateParser.toJson(update, SPECS_BY_ID);
    assertThat(json).contains("\"action\":\"produce-snapshot\"");
    assertThat(json).contains("\"snapshot-action\":\"append\"");
    assertThat(json).contains("\"add-data-files\"");
    assertThat(json).contains("\"base-snapshot-id\":123456789");
    assertThat(json).contains("\"branch\":\"main\"");
    assertThat(json).contains("\"summary\":{\"key\":\"value\"}");

    MetadataUpdate parsed = MetadataUpdateParser.fromJson(json);
    assertThat(parsed).isInstanceOf(MetadataUpdate.ProduceSnapshotUpdate.class);

    MetadataUpdate.ProduceSnapshotUpdate result = (MetadataUpdate.ProduceSnapshotUpdate) parsed;
    assertThat(result.isUnresolved()).isTrue();
  }

  @Test
  public void testProduceSnapshotUpdateWithUnresolvedFiles() {
    MetadataUpdate.ProduceSnapshotUpdate update =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.OVERWRITE,
            ImmutableList.of(TestBase.FILE_A, TestBase.FILE_B),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            false,
            null,
            null,
            null,
            null);

    String json = MetadataUpdateParser.toJson(update, SPECS_BY_ID);
    MetadataUpdate parsed = MetadataUpdateParser.fromJson(json);
    assertThat(parsed).isInstanceOf(MetadataUpdate.ProduceSnapshotUpdate.class);

    MetadataUpdate.ProduceSnapshotUpdate result = (MetadataUpdate.ProduceSnapshotUpdate) parsed;
    assertThat(result.isUnresolved()).isTrue();

    // Resolve and verify files are parsed correctly
    MetadataUpdate.ProduceSnapshotUpdate resolved = result.resolve(SPECS_BY_ID);
    assertThat(resolved.isUnresolved()).isFalse();
    assertThat(resolved.addDataFiles()).hasSize(2);
  }

  @Test
  public void testProduceSnapshotUpdateWithDeleteFiles() {
    MetadataUpdate.ProduceSnapshotUpdate update =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.DELETE,
            Collections.emptyList(),
            ImmutableList.of(TestBase.FILE_A_DELETES),
            Collections.emptyList(),
            Collections.emptyList(),
            Expressions.equal("id", 5),
            false,
            null,
            null,
            null,
            null);

    String json = MetadataUpdateParser.toJson(update, SPECS_BY_ID);
    assertThat(json).contains("\"snapshot-action\":\"delete\"");
    assertThat(json).contains("\"add-delete-files\"");
    assertThat(json).contains("\"delete-row-filter\"");

    MetadataUpdate parsed = MetadataUpdateParser.fromJson(json);
    MetadataUpdate.ProduceSnapshotUpdate result = (MetadataUpdate.ProduceSnapshotUpdate) parsed;
    assertThat(result.action()).isEqualTo("delete");
    assertThat(result.deleteRowFilter()).isNotNull();
  }

  @Test
  public void testProduceSnapshotUpdateWithStageOnly() {
    MetadataUpdate.ProduceSnapshotUpdate update =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.APPEND,
            ImmutableList.of(TestBase.FILE_A),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            true,
            null,
            null,
            null,
            null);

    String json = MetadataUpdateParser.toJson(update, SPECS_BY_ID);
    assertThat(json).contains("\"stage-only\":true");

    MetadataUpdate parsed = MetadataUpdateParser.fromJson(json);
    MetadataUpdate.ProduceSnapshotUpdate result = (MetadataUpdate.ProduceSnapshotUpdate) parsed;
    assertThat(result.stageOnly()).isTrue();
  }

  @Test
  public void testProduceSnapshotUpdateWithRemoveDataFiles() {
    MetadataUpdate.ProduceSnapshotUpdate update =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.REPLACE,
            ImmutableList.of(TestBase.FILE_B),
            Collections.emptyList(),
            ImmutableList.of(TestBase.FILE_A),
            Collections.emptyList(),
            null,
            false,
            null,
            null,
            null,
            null);

    String json = MetadataUpdateParser.toJson(update, SPECS_BY_ID);
    assertThat(json).contains("\"snapshot-action\":\"replace\"");
    assertThat(json).contains("\"add-data-files\"");
    assertThat(json).contains("\"remove-data-files\"");

    MetadataUpdate parsed = MetadataUpdateParser.fromJson(json);
    MetadataUpdate.ProduceSnapshotUpdate result = (MetadataUpdate.ProduceSnapshotUpdate) parsed;
    assertThat(result.action()).isEqualTo("replace");
  }

  @Test
  public void testProduceSnapshotUpdateNullAction() {
    assertThatThrownBy(
            () ->
                new MetadataUpdate.ProduceSnapshotUpdate(
                    null,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    null,
                    false,
                    null,
                    null,
                    null,
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Snapshot action cannot be null");
  }

  @Test
  public void testProduceSnapshotUpdateMinimalFields() {
    MetadataUpdate.ProduceSnapshotUpdate update =
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

    String json = MetadataUpdateParser.toJson(update, SPECS_BY_ID);
    assertThat(json).contains("\"action\":\"produce-snapshot\"");
    assertThat(json).contains("\"snapshot-action\":\"append\"");

    MetadataUpdate parsed = MetadataUpdateParser.fromJson(json);
    assertThat(parsed).isInstanceOf(MetadataUpdate.ProduceSnapshotUpdate.class);
  }

  @Test
  public void testProduceSnapshotUpdateToJsonWithoutSpecs() {
    MetadataUpdate.ProduceSnapshotUpdate update =
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

    // Test that toJson without specs throws
    assertThatThrownBy(() -> MetadataUpdateParser.toJson(update))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot serialize resolved ProduceSnapshotUpdate without partition specs");
  }

  @Test
  public void testProduceSnapshotUpdateResolve() {
    // Create an unresolved update from JSON
    String json =
        "{\"action\":\"produce-snapshot\",\"snapshot-action\":\"append\","
            + "\"stage-only\":false,"
            + "\"add-data-files\":[{\"spec-id\":0,\"content\":\"data\","
            + "\"file-path\":\"/path/to/data.parquet\",\"file-format\":\"parquet\","
            + "\"partition\":[1],\"file-size-in-bytes\":10,\"record-count\":1}]}";

    MetadataUpdate parsed = MetadataUpdateParser.fromJson(json);
    MetadataUpdate.ProduceSnapshotUpdate unresolved = (MetadataUpdate.ProduceSnapshotUpdate) parsed;
    assertThat(unresolved.isUnresolved()).isTrue();

    MetadataUpdate.ProduceSnapshotUpdate resolved = unresolved.resolve(SPECS_BY_ID);
    assertThat(resolved.isUnresolved()).isFalse();
    assertThat(resolved.addDataFiles()).hasSize(1);
    assertThat(resolved.addDataFiles().get(0).path().toString()).isEqualTo("/path/to/data.parquet");
  }

  @Test
  public void testProduceSnapshotUpdateStageOnlyDefaultsToFalse() {
    // Test that stage-only defaults to false when not provided
    String json =
        "{\"action\":\"produce-snapshot\",\"snapshot-action\":\"append\","
            + "\"add-data-files\":[{\"spec-id\":0,\"content\":\"data\","
            + "\"file-path\":\"/path/to/data.parquet\",\"file-format\":\"parquet\","
            + "\"partition\":[1],\"file-size-in-bytes\":10,\"record-count\":1}]}";

    MetadataUpdate parsed = MetadataUpdateParser.fromJson(json);
    MetadataUpdate.ProduceSnapshotUpdate result = (MetadataUpdate.ProduceSnapshotUpdate) parsed;
    assertThat(result.stageOnly()).isFalse();
  }

  @Test
  public void testProduceSnapshotUpdateWithAllFields() {
    MetadataUpdate.ProduceSnapshotUpdate update =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.OVERWRITE,
            ImmutableList.of(TestBase.FILE_A),
            ImmutableList.of(TestBase.FILE_A_DELETES),
            ImmutableList.of(TestBase.FILE_B),
            Collections.emptyList(),
            Expressions.greaterThan("id", 10),
            true,
            "feature-branch",
            ImmutableMap.of("commit-id", "abc123", "author", "test"),
            987654321L,
            null);

    String json = MetadataUpdateParser.toJson(update, SPECS_BY_ID);
    assertThat(json).contains("\"snapshot-action\":\"overwrite\"");
    assertThat(json).contains("\"add-data-files\"");
    assertThat(json).contains("\"add-delete-files\"");
    assertThat(json).contains("\"remove-data-files\"");
    assertThat(json).contains("\"delete-row-filter\"");
    assertThat(json).contains("\"stage-only\":true");
    assertThat(json).contains("\"branch\":\"feature-branch\"");
    assertThat(json).contains("\"commit-id\":\"abc123\"");
    assertThat(json).contains("\"base-snapshot-id\":987654321");

    MetadataUpdate parsed = MetadataUpdateParser.fromJson(json);
    assertThat(parsed).isInstanceOf(MetadataUpdate.ProduceSnapshotUpdate.class);

    MetadataUpdate.ProduceSnapshotUpdate result = (MetadataUpdate.ProduceSnapshotUpdate) parsed;
    assertThat(result.action()).isEqualTo("overwrite");
    assertThat(result.stageOnly()).isTrue();
    assertThat(result.branch()).isEqualTo("feature-branch");
    assertThat(result.summary()).containsEntry("commit-id", "abc123");
    assertThat(result.baseSnapshotId()).isEqualTo(987654321L);
  }

  @Test
  public void testProduceSnapshotUpdateWithComplexExpression() {
    MetadataUpdate.ProduceSnapshotUpdate update =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.DELETE,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Expressions.and(
                Expressions.greaterThan("id", 100), Expressions.lessThanOrEqual("id", 200)),
            false,
            null,
            null,
            null,
            null);

    String json = MetadataUpdateParser.toJson(update, SPECS_BY_ID);
    assertThat(json).contains("\"delete-row-filter\"");
    assertThat(json).contains("\"and\"");

    MetadataUpdate parsed = MetadataUpdateParser.fromJson(json);
    MetadataUpdate.ProduceSnapshotUpdate result = (MetadataUpdate.ProduceSnapshotUpdate) parsed;
    assertThat(result.deleteRowFilter()).isNotNull();
    assertThat(result.deleteRowFilter().toString()).contains("and");
  }

  @Test
  public void testProduceSnapshotUpdateWithValidations() {
    CommitValidation validation1 =
        new CommitValidation.NotAllowedAddedDataFiles(Expressions.equal("partition_key", "value"));
    CommitValidation validation2 =
        new CommitValidation.NotAllowedAddedDeleteFiles(Expressions.alwaysTrue());

    MetadataUpdate.ProduceSnapshotUpdate update =
        new MetadataUpdate.ProduceSnapshotUpdate(
            DataOperations.OVERWRITE,
            ImmutableList.of(TestBase.FILE_A),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            false,
            "main",
            null,
            123456789L,
            ImmutableList.of(validation1, validation2));

    String json = MetadataUpdateParser.toJson(update, SPECS_BY_ID);
    assertThat(json).contains("\"commit-validations\"");
    assertThat(json).contains("\"not-allowed-added-data-files\"");
    assertThat(json).contains("\"not-allowed-added-delete-files\"");

    MetadataUpdate parsed = MetadataUpdateParser.fromJson(json);
    MetadataUpdate.ProduceSnapshotUpdate result = (MetadataUpdate.ProduceSnapshotUpdate) parsed;
    assertThat(result.validations()).hasSize(2);
    assertThat(result.validations().get(0)).isInstanceOf(CommitValidation.NotAllowedAddedDataFiles.class);
    assertThat(result.validations().get(1)).isInstanceOf(CommitValidation.NotAllowedAddedDeleteFiles.class);
  }

  @Test
  public void testProduceSnapshotUpdateWithEmptyValidations() {
    MetadataUpdate.ProduceSnapshotUpdate update =
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
            Collections.emptyList());

    String json = MetadataUpdateParser.toJson(update, SPECS_BY_ID);
    // Empty validations should not be written
    assertThat(json).doesNotContain("\"commit-validations\"");

    MetadataUpdate parsed = MetadataUpdateParser.fromJson(json);
    MetadataUpdate.ProduceSnapshotUpdate result = (MetadataUpdate.ProduceSnapshotUpdate) parsed;
    assertThat(result.validations()).isEmpty();
  }
}
