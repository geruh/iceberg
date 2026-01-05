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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

public class TestCommitValidationParser {

  @Test
  public void testNullValidation() {
    assertThatThrownBy(() -> CommitValidationParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid commit validation: null");
  }

  @Test
  public void testNullJsonNode() {
    assertThatThrownBy(() -> CommitValidationParser.fromJson((String) null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testMissingType() {
    assertThatThrownBy(() -> CommitValidationParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Missing field: type");
  }

  @Test
  public void testUnknownType() {
    assertThatThrownBy(() -> CommitValidationParser.fromJson("{\"type\":\"unknown-type\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown commit validation type: unknown-type");
  }

  @Test
  public void testNotAllowedAddedDataFilesRoundTrip() {
    CommitValidation validation =
        new CommitValidation.NotAllowedAddedDataFiles(Expressions.equal("id", 5));

    String json = CommitValidationParser.toJson(validation);
    assertThat(json).contains("\"type\":\"not-allowed-added-data-files\"");
    assertThat(json).contains("\"filter\"");

    CommitValidation parsed = CommitValidationParser.fromJson(json);
    assertThat(parsed).isInstanceOf(CommitValidation.NotAllowedAddedDataFiles.class);
    CommitValidation.NotAllowedAddedDataFiles result =
        (CommitValidation.NotAllowedAddedDataFiles) parsed;
    assertThat(result.type()).isEqualTo(CommitValidation.NotAllowedAddedDataFiles.TYPE);
    assertThat(result.filter()).isNotNull();
  }

  @Test
  public void testNotAllowedAddedDataFilesAlwaysTrue() {
    CommitValidation validation =
        new CommitValidation.NotAllowedAddedDataFiles(Expressions.alwaysTrue());

    String json = CommitValidationParser.toJson(validation);
    CommitValidation parsed = CommitValidationParser.fromJson(json);

    assertThat(parsed).isInstanceOf(CommitValidation.NotAllowedAddedDataFiles.class);
    CommitValidation.NotAllowedAddedDataFiles result =
        (CommitValidation.NotAllowedAddedDataFiles) parsed;
    assertThat(result.filter().toString()).isEqualTo("true");
  }

  @Test
  public void testNotAllowedAddedDataFilesMissingFilter() {
    assertThatThrownBy(
            () ->
                CommitValidationParser.fromJson(
                    "{\"type\":\"not-allowed-added-data-files\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Missing required field: filter");
  }

  @Test
  public void testNotAllowedAddedDeleteFilesRoundTrip() {
    CommitValidation validation =
        new CommitValidation.NotAllowedAddedDeleteFiles(
            Expressions.and(Expressions.equal("category", "A"), Expressions.greaterThan("ts", 100)));

    String json = CommitValidationParser.toJson(validation);
    assertThat(json).contains("\"type\":\"not-allowed-added-delete-files\"");
    assertThat(json).contains("\"filter\"");

    CommitValidation parsed = CommitValidationParser.fromJson(json);
    assertThat(parsed).isInstanceOf(CommitValidation.NotAllowedAddedDeleteFiles.class);
    CommitValidation.NotAllowedAddedDeleteFiles result =
        (CommitValidation.NotAllowedAddedDeleteFiles) parsed;
    assertThat(result.type()).isEqualTo(CommitValidation.NotAllowedAddedDeleteFiles.TYPE);
    assertThat(result.filter()).isNotNull();
  }

  @Test
  public void testNotAllowedAddedDeleteFilesMissingFilter() {
    assertThatThrownBy(
            () ->
                CommitValidationParser.fromJson(
                    "{\"type\":\"not-allowed-added-delete-files\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Missing required field: filter");
  }

  @Test
  public void testRequiredDataFilesWithFilterRoundTrip() {
    CommitValidation validation =
        new CommitValidation.RequiredDataFiles(
            Expressions.lessThan("date", "2024-01-01"), null);

    String json = CommitValidationParser.toJson(validation);
    assertThat(json).contains("\"type\":\"required-data-files\"");
    assertThat(json).contains("\"filter\"");
    assertThat(json).doesNotContain("\"file-paths\"");

    CommitValidation parsed = CommitValidationParser.fromJson(json);
    assertThat(parsed).isInstanceOf(CommitValidation.RequiredDataFiles.class);
    CommitValidation.RequiredDataFiles result = (CommitValidation.RequiredDataFiles) parsed;
    assertThat(result.type()).isEqualTo(CommitValidation.RequiredDataFiles.TYPE);
    assertThat(result.filter()).isNotNull();
    assertThat(result.filePaths()).isNull();
  }

  @Test
  public void testRequiredDataFilesWithFilePathsRoundTrip() {
    List<String> filePaths =
        ImmutableList.of(
            "s3://bucket/table/data/file1.parquet", "s3://bucket/table/data/file2.parquet");

    CommitValidation validation = new CommitValidation.RequiredDataFiles(null, filePaths);

    String json = CommitValidationParser.toJson(validation);
    assertThat(json).contains("\"type\":\"required-data-files\"");
    assertThat(json).contains("\"file-paths\"");
    assertThat(json).contains("file1.parquet");
    assertThat(json).contains("file2.parquet");
    assertThat(json).doesNotContain("\"filter\"");

    CommitValidation parsed = CommitValidationParser.fromJson(json);
    assertThat(parsed).isInstanceOf(CommitValidation.RequiredDataFiles.class);
    CommitValidation.RequiredDataFiles result = (CommitValidation.RequiredDataFiles) parsed;
    assertThat(result.filter()).isNull();
    assertThat(result.filePaths()).containsExactlyElementsOf(filePaths);
  }

  @Test
  public void testRequiredDataFilesWithAllowedRemoveOperationsRoundTrip() {
    List<String> filePaths = ImmutableList.of("s3://bucket/table/data/file1.parquet");

    CommitValidation validation =
        new CommitValidation.RequiredDataFiles(
            null, filePaths, ImmutableSet.of("delete", "overwrite"));

    String json = CommitValidationParser.toJson(validation);
    assertThat(json).contains("\"allowed-remove-operations\"");
    assertThat(json).contains("delete");
    assertThat(json).contains("overwrite");

    CommitValidation parsed = CommitValidationParser.fromJson(json);
    assertThat(parsed).isInstanceOf(CommitValidation.RequiredDataFiles.class);
    CommitValidation.RequiredDataFiles result = (CommitValidation.RequiredDataFiles) parsed;
    assertThat(result.allowedRemoveOperations()).containsExactlyInAnyOrder("delete", "overwrite");
  }

  @Test
  public void testRequiredDataFilesWithBothFilterAndFilePaths() {
    List<String> filePaths = ImmutableList.of("s3://bucket/table/data/file1.parquet");

    CommitValidation validation =
        new CommitValidation.RequiredDataFiles(Expressions.equal("id", 1), filePaths);

    String json = CommitValidationParser.toJson(validation);
    assertThat(json).contains("\"filter\"");
    assertThat(json).contains("\"file-paths\"");

    CommitValidation parsed = CommitValidationParser.fromJson(json);
    assertThat(parsed).isInstanceOf(CommitValidation.RequiredDataFiles.class);
    CommitValidation.RequiredDataFiles result = (CommitValidation.RequiredDataFiles) parsed;
    assertThat(result.filter()).isNotNull();
    assertThat(result.filePaths()).hasSize(1);
  }

  @Test
  public void testRequiredDeleteFilesWithFilterRoundTrip() {
    CommitValidation validation =
        new CommitValidation.RequiredDeleteFiles(Expressions.greaterThan("count", 10), null);

    String json = CommitValidationParser.toJson(validation);
    assertThat(json).contains("\"type\":\"required-delete-files\"");
    assertThat(json).contains("\"filter\"");

    CommitValidation parsed = CommitValidationParser.fromJson(json);
    assertThat(parsed).isInstanceOf(CommitValidation.RequiredDeleteFiles.class);
    CommitValidation.RequiredDeleteFiles result = (CommitValidation.RequiredDeleteFiles) parsed;
    assertThat(result.type()).isEqualTo(CommitValidation.RequiredDeleteFiles.TYPE);
    assertThat(result.filter()).isNotNull();
    assertThat(result.filePaths()).isNull();
  }

  @Test
  public void testRequiredDeleteFilesWithFilePathsRoundTrip() {
    List<String> filePaths =
        ImmutableList.of(
            "s3://bucket/table/data/delete1.parquet", "s3://bucket/table/data/delete2.parquet");

    CommitValidation validation = new CommitValidation.RequiredDeleteFiles(null, filePaths);

    String json = CommitValidationParser.toJson(validation);
    assertThat(json).contains("\"type\":\"required-delete-files\"");
    assertThat(json).contains("\"file-paths\"");

    CommitValidation parsed = CommitValidationParser.fromJson(json);
    assertThat(parsed).isInstanceOf(CommitValidation.RequiredDeleteFiles.class);
    CommitValidation.RequiredDeleteFiles result = (CommitValidation.RequiredDeleteFiles) parsed;
    assertThat(result.filePaths()).containsExactlyElementsOf(filePaths);
  }

  @Test
  public void testNotAllowedNewDeletesForDataFilesWithFilePathsRoundTrip() {
    List<String> filePaths =
        ImmutableList.of("s3://bucket/table/data/data1.parquet");

    CommitValidation validation =
        new CommitValidation.NotAllowedNewDeletesForDataFiles(filePaths, null);

    String json = CommitValidationParser.toJson(validation);
    assertThat(json).contains("\"type\":\"not-allowed-new-deletes-for-data-files\"");
    assertThat(json).contains("\"file-paths\"");

    CommitValidation parsed = CommitValidationParser.fromJson(json);
    assertThat(parsed).isInstanceOf(CommitValidation.NotAllowedNewDeletesForDataFiles.class);
    CommitValidation.NotAllowedNewDeletesForDataFiles result =
        (CommitValidation.NotAllowedNewDeletesForDataFiles) parsed;
    assertThat(result.type()).isEqualTo(CommitValidation.NotAllowedNewDeletesForDataFiles.TYPE);
    assertThat(result.filePaths()).containsExactlyElementsOf(filePaths);
    assertThat(result.filter()).isNull();
  }

  @Test
  public void testNotAllowedNewDeletesForDataFilesWithFilterRoundTrip() {
    CommitValidation validation =
        new CommitValidation.NotAllowedNewDeletesForDataFiles(
            null, Expressions.equal("partition_col", "value"));

    String json = CommitValidationParser.toJson(validation);
    assertThat(json).contains("\"type\":\"not-allowed-new-deletes-for-data-files\"");
    assertThat(json).contains("\"filter\"");

    CommitValidation parsed = CommitValidationParser.fromJson(json);
    assertThat(parsed).isInstanceOf(CommitValidation.NotAllowedNewDeletesForDataFiles.class);
    CommitValidation.NotAllowedNewDeletesForDataFiles result =
        (CommitValidation.NotAllowedNewDeletesForDataFiles) parsed;
    assertThat(result.filter()).isNotNull();
    assertThat(result.filePaths()).isNull();
  }

  @Test
  public void testNotAllowedNewDeletesForDataFilesWithBoth() {
    List<String> filePaths = ImmutableList.of("s3://bucket/table/data/data1.parquet");

    CommitValidation validation =
        new CommitValidation.NotAllowedNewDeletesForDataFiles(
            filePaths, Expressions.alwaysTrue());

    String json = CommitValidationParser.toJson(validation);
    assertThat(json).contains("\"file-paths\"");
    assertThat(json).contains("\"filter\"");

    CommitValidation parsed = CommitValidationParser.fromJson(json);
    assertThat(parsed).isInstanceOf(CommitValidation.NotAllowedNewDeletesForDataFiles.class);
    CommitValidation.NotAllowedNewDeletesForDataFiles result =
        (CommitValidation.NotAllowedNewDeletesForDataFiles) parsed;
    assertThat(result.filePaths()).hasSize(1);
    assertThat(result.filter()).isNotNull();
  }

  @Test
  public void testPrettyJson() {
    CommitValidation validation =
        new CommitValidation.NotAllowedAddedDataFiles(Expressions.alwaysTrue());

    String prettyJson = CommitValidationParser.toJson(validation, true);
    assertThat(prettyJson).contains("\n");
    assertThat(prettyJson).contains("  ");
  }

  @Test
  public void testCaseInsensitiveType() {
    // alwaysTrue() is serialized as "true" directly, not as a JSON object
    String upperCaseJson = "{\"type\":\"NOT-ALLOWED-ADDED-DATA-FILES\",\"filter\":true}";
    CommitValidation parsed = CommitValidationParser.fromJson(upperCaseJson);
    assertThat(parsed).isInstanceOf(CommitValidation.NotAllowedAddedDataFiles.class);
  }
}
