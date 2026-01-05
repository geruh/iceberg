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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.util.JsonUtil;

/** Parser for {@link CommitValidation} objects to and from JSON. */
public class CommitValidationParser {

  private static final String TYPE = "type";
  private static final String FILTER = "filter";
  private static final String FILE_PATHS = "file-paths";
  private static final String ALLOWED_REMOVE_OPERATIONS = "allowed-remove-operations";

  private CommitValidationParser() {}

  public static String toJson(CommitValidation validation) {
    return toJson(validation, false);
  }

  public static String toJson(CommitValidation validation, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(validation, gen), pretty);
  }

  public static void toJson(CommitValidation validation, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(validation != null, "Invalid commit validation: null");

    gen.writeStartObject();
    gen.writeStringField(TYPE, validation.type());

    if (validation instanceof CommitValidation.NotAllowedAddedDataFiles) {
      writeNotAllowedAddedDataFiles(
          (CommitValidation.NotAllowedAddedDataFiles) validation, gen);
    } else if (validation instanceof CommitValidation.NotAllowedAddedDeleteFiles) {
      writeNotAllowedAddedDeleteFiles(
          (CommitValidation.NotAllowedAddedDeleteFiles) validation, gen);
    } else if (validation instanceof CommitValidation.RequiredDataFiles) {
      writeRequiredDataFiles((CommitValidation.RequiredDataFiles) validation, gen);
    } else if (validation instanceof CommitValidation.RequiredDeleteFiles) {
      writeRequiredDeleteFiles((CommitValidation.RequiredDeleteFiles) validation, gen);
    } else if (validation instanceof CommitValidation.NotAllowedNewDeletesForDataFiles) {
      writeNotAllowedNewDeletesForDataFiles(
          (CommitValidation.NotAllowedNewDeletesForDataFiles) validation, gen);
    } else {
      throw new IllegalArgumentException(
          "Cannot serialize unknown commit validation type: " + validation.getClass().getName());
    }

    gen.writeEndObject();
  }

  private static void writeNotAllowedAddedDataFiles(
      CommitValidation.NotAllowedAddedDataFiles validation, JsonGenerator gen) throws IOException {
    gen.writeFieldName(FILTER);
    ExpressionParser.toJson(validation.filter(), gen);
  }

  private static void writeNotAllowedAddedDeleteFiles(
      CommitValidation.NotAllowedAddedDeleteFiles validation, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(FILTER);
    ExpressionParser.toJson(validation.filter(), gen);
  }

  private static void writeRequiredDataFiles(
      CommitValidation.RequiredDataFiles validation, JsonGenerator gen) throws IOException {
    if (validation.filter() != null) {
      gen.writeFieldName(FILTER);
      ExpressionParser.toJson(validation.filter(), gen);
    }
    if (validation.filePaths() != null && !validation.filePaths().isEmpty()) {
      JsonUtil.writeStringArray(FILE_PATHS, validation.filePaths(), gen);
    }
    if (validation.allowedRemoveOperations() != null
        && !validation.allowedRemoveOperations().isEmpty()) {
      JsonUtil.writeStringArray(ALLOWED_REMOVE_OPERATIONS, validation.allowedRemoveOperations(), gen);
    }
  }

  private static void writeRequiredDeleteFiles(
      CommitValidation.RequiredDeleteFiles validation, JsonGenerator gen) throws IOException {
    if (validation.filter() != null) {
      gen.writeFieldName(FILTER);
      ExpressionParser.toJson(validation.filter(), gen);
    }
    if (validation.filePaths() != null && !validation.filePaths().isEmpty()) {
      JsonUtil.writeStringArray(FILE_PATHS, validation.filePaths(), gen);
    }
  }

  private static void writeNotAllowedNewDeletesForDataFiles(
      CommitValidation.NotAllowedNewDeletesForDataFiles validation, JsonGenerator gen)
      throws IOException {
    if (validation.filePaths() != null && !validation.filePaths().isEmpty()) {
      JsonUtil.writeStringArray(FILE_PATHS, validation.filePaths(), gen);
    }
    if (validation.filter() != null) {
      gen.writeFieldName(FILTER);
      ExpressionParser.toJson(validation.filter(), gen);
    }
  }

  public static CommitValidation fromJson(String json) {
    return JsonUtil.parse(json, CommitValidationParser::fromJson);
  }

  public static CommitValidation fromJson(JsonNode json) {
    Preconditions.checkArgument(
        json != null && json.isObject(),
        "Cannot parse commit validation from non-object: %s",
        json);
    Preconditions.checkArgument(
        json.hasNonNull(TYPE), "Cannot parse commit validation. Missing field: type");

    String type = JsonUtil.getString(TYPE, json).toLowerCase(Locale.ROOT);

    switch (type) {
      case CommitValidation.NotAllowedAddedDataFiles.TYPE:
        return readNotAllowedAddedDataFiles(json);
      case CommitValidation.NotAllowedAddedDeleteFiles.TYPE:
        return readNotAllowedAddedDeleteFiles(json);
      case CommitValidation.RequiredDataFiles.TYPE:
        return readRequiredDataFiles(json);
      case CommitValidation.RequiredDeleteFiles.TYPE:
        return readRequiredDeleteFiles(json);
      case CommitValidation.NotAllowedNewDeletesForDataFiles.TYPE:
        return readNotAllowedNewDeletesForDataFiles(json);
      default:
        throw new IllegalArgumentException("Unknown commit validation type: " + type);
    }
  }

  private static CommitValidation.NotAllowedAddedDataFiles readNotAllowedAddedDataFiles(
      JsonNode json) {
    Preconditions.checkArgument(
        json.hasNonNull(FILTER), "Missing required field: %s", FILTER);
    Expression filter = ExpressionParser.fromJson(json.get(FILTER));
    return new CommitValidation.NotAllowedAddedDataFiles(filter);
  }

  private static CommitValidation.NotAllowedAddedDeleteFiles readNotAllowedAddedDeleteFiles(
      JsonNode json) {
    Preconditions.checkArgument(
        json.hasNonNull(FILTER), "Missing required field: %s", FILTER);
    Expression filter = ExpressionParser.fromJson(json.get(FILTER));
    return new CommitValidation.NotAllowedAddedDeleteFiles(filter);
  }

  private static CommitValidation.RequiredDataFiles readRequiredDataFiles(JsonNode json) {
    Expression filter = null;
    if (json.hasNonNull(FILTER)) {
      filter = ExpressionParser.fromJson(json.get(FILTER));
    }

    List<String> filePaths = null;
    if (json.hasNonNull(FILE_PATHS)) {
      filePaths = JsonUtil.getStringList(FILE_PATHS, json);
    }

    Set<String> allowedRemoveOperations = null;
    if (json.hasNonNull(ALLOWED_REMOVE_OPERATIONS)) {
      allowedRemoveOperations =
          ImmutableSet.copyOf(JsonUtil.getStringList(ALLOWED_REMOVE_OPERATIONS, json));
    }

    return new CommitValidation.RequiredDataFiles(filter, filePaths, allowedRemoveOperations);
  }

  private static CommitValidation.RequiredDeleteFiles readRequiredDeleteFiles(JsonNode json) {
    Expression filter = null;
    if (json.hasNonNull(FILTER)) {
      filter = ExpressionParser.fromJson(json.get(FILTER));
    }

    List<String> filePaths = null;
    if (json.hasNonNull(FILE_PATHS)) {
      filePaths = JsonUtil.getStringList(FILE_PATHS, json);
    }

    return new CommitValidation.RequiredDeleteFiles(filter, filePaths);
  }

  private static CommitValidation.NotAllowedNewDeletesForDataFiles
      readNotAllowedNewDeletesForDataFiles(JsonNode json) {
    List<String> filePaths = null;
    if (json.hasNonNull(FILE_PATHS)) {
      filePaths = JsonUtil.getStringList(FILE_PATHS, json);
    }

    Expression filter = null;
    if (json.hasNonNull(FILTER)) {
      filter = ExpressionParser.fromJson(json.get(FILTER));
    }

    return new CommitValidation.NotAllowedNewDeletesForDataFiles(filePaths, filter);
  }
}
