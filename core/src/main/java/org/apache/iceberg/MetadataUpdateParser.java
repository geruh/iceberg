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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.view.ViewVersionParser;

public class MetadataUpdateParser {

  private MetadataUpdateParser() {}

  private static final String ACTION = "action";

  // action types - visible for testing
  static final String ASSIGN_UUID = "assign-uuid";
  static final String UPGRADE_FORMAT_VERSION = "upgrade-format-version";
  static final String ADD_SCHEMA = "add-schema";
  static final String SET_CURRENT_SCHEMA = "set-current-schema";
  static final String ADD_PARTITION_SPEC = "add-spec";
  static final String SET_DEFAULT_PARTITION_SPEC = "set-default-spec";
  static final String ADD_SORT_ORDER = "add-sort-order";
  static final String SET_DEFAULT_SORT_ORDER = "set-default-sort-order";
  static final String ADD_SNAPSHOT = "add-snapshot";
  static final String REMOVE_SNAPSHOTS = "remove-snapshots";
  static final String REMOVE_SNAPSHOT_REF = "remove-snapshot-ref";
  static final String SET_SNAPSHOT_REF = "set-snapshot-ref";
  static final String SET_PROPERTIES = "set-properties";
  static final String REMOVE_PROPERTIES = "remove-properties";
  static final String SET_LOCATION = "set-location";
  static final String SET_STATISTICS = "set-statistics";
  static final String REMOVE_STATISTICS = "remove-statistics";
  static final String ADD_VIEW_VERSION = "add-view-version";
  static final String SET_CURRENT_VIEW_VERSION = "set-current-view-version";
  static final String SET_PARTITION_STATISTICS = "set-partition-statistics";
  static final String REMOVE_PARTITION_STATISTICS = "remove-partition-statistics";
  static final String REMOVE_PARTITION_SPECS = "remove-partition-specs";
  static final String REMOVE_SCHEMAS = "remove-schemas";
  static final String ADD_ENCRYPTION_KEY = "add-encryption-key";
  static final String REMOVE_ENCRYPTION_KEY = "remove-encryption-key";
  static final String PRODUCE_SNAPSHOT = "produce-snapshot";

  // AssignUUID
  private static final String UUID = "uuid";

  // UpgradeFormatVersion
  private static final String FORMAT_VERSION = "format-version";

  // AddSchema
  private static final String SCHEMA = "schema";
  private static final String LAST_COLUMN_ID = "last-column-id";

  // SetCurrentSchema
  private static final String SCHEMA_ID = "schema-id";

  // AddPartitionSpec
  private static final String SPEC = "spec";

  // SetDefaultPartitionSpec
  private static final String SPEC_ID = "spec-id";

  // AddSortOrder
  private static final String SORT_ORDER = "sort-order";

  // SetDefaultSortOrder
  private static final String SORT_ORDER_ID = "sort-order-id";

  // SetStatistics
  private static final String STATISTICS = "statistics";

  // SetPartitionStatistics
  private static final String PARTITION_STATISTICS = "partition-statistics";

  // AddSnapshot
  private static final String SNAPSHOT = "snapshot";

  // RemoveSnapshots
  private static final String SNAPSHOT_IDS = "snapshot-ids";

  // SetSnapshotRef
  private static final String REF_NAME = "ref-name"; // Also used in RemoveSnapshotRef
  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String TYPE = "type";
  private static final String MIN_SNAPSHOTS_TO_KEEP = "min-snapshots-to-keep";
  private static final String MAX_SNAPSHOT_AGE_MS = "max-snapshot-age-ms";
  private static final String MAX_REF_AGE_MS = "max-ref-age-ms";

  // SetProperties
  // the REST API Spec defines "updates" but we initially used "updated",
  // thus we need to support reading both indefinitely
  private static final String UPDATED = "updated";
  private static final String UPDATES = "updates";

  // RemoveProperties
  // the REST API Spec defines "removals" but we initially used "removed",
  // thus we need to support reading both indefinitely
  private static final String REMOVED = "removed";
  private static final String REMOVALS = "removals";

  // SetLocation
  private static final String LOCATION = "location";

  // AddViewVersion
  private static final String VIEW_VERSION = "view-version";

  // SetCurrentViewVersion
  private static final String VIEW_VERSION_ID = "view-version-id";

  // RemovePartitionSpecs
  private static final String SPEC_IDS = "spec-ids";

  // RemoveSchemas
  private static final String SCHEMA_IDS = "schema-ids";

  // AddEncryptionKey
  private static final String ENCRYPTION_KEY = "encryption-key";

  // RemoveEncryptionKey
  private static final String KEY_ID = "key-id";

  // ProduceSnapshotUpdate
  private static final String SNAPSHOT_ACTION = "snapshot-action";
  private static final String ADD_DATA_FILES = "add-data-files";
  private static final String ADD_DELETE_FILES = "add-delete-files";
  private static final String REMOVE_DATA_FILES = "remove-data-files";
  private static final String REMOVE_DELETE_FILES = "remove-delete-files";
  private static final String DELETE_ROW_FILTER = "delete-row-filter";
  private static final String STAGE_ONLY = "stage-only";
  private static final String BRANCH = "branch";
  private static final String SUMMARY = "summary";
  private static final String BASE_SNAPSHOT_ID = "base-snapshot-id";
  private static final String COMMIT_VALIDATIONS = "commit-validations";

  private static final Map<Class<? extends MetadataUpdate>, String> ACTIONS =
      ImmutableMap.<Class<? extends MetadataUpdate>, String>builder()
          .put(MetadataUpdate.AssignUUID.class, ASSIGN_UUID)
          .put(MetadataUpdate.UpgradeFormatVersion.class, UPGRADE_FORMAT_VERSION)
          .put(MetadataUpdate.AddSchema.class, ADD_SCHEMA)
          .put(MetadataUpdate.SetCurrentSchema.class, SET_CURRENT_SCHEMA)
          .put(MetadataUpdate.AddPartitionSpec.class, ADD_PARTITION_SPEC)
          .put(MetadataUpdate.SetDefaultPartitionSpec.class, SET_DEFAULT_PARTITION_SPEC)
          .put(MetadataUpdate.AddSortOrder.class, ADD_SORT_ORDER)
          .put(MetadataUpdate.SetDefaultSortOrder.class, SET_DEFAULT_SORT_ORDER)
          .put(MetadataUpdate.SetStatistics.class, SET_STATISTICS)
          .put(MetadataUpdate.RemoveStatistics.class, REMOVE_STATISTICS)
          .put(MetadataUpdate.SetPartitionStatistics.class, SET_PARTITION_STATISTICS)
          .put(MetadataUpdate.RemovePartitionStatistics.class, REMOVE_PARTITION_STATISTICS)
          .put(MetadataUpdate.AddSnapshot.class, ADD_SNAPSHOT)
          .put(MetadataUpdate.RemoveSnapshots.class, REMOVE_SNAPSHOTS)
          .put(MetadataUpdate.RemoveSnapshotRef.class, REMOVE_SNAPSHOT_REF)
          .put(MetadataUpdate.SetSnapshotRef.class, SET_SNAPSHOT_REF)
          .put(MetadataUpdate.SetProperties.class, SET_PROPERTIES)
          .put(MetadataUpdate.RemoveProperties.class, REMOVE_PROPERTIES)
          .put(MetadataUpdate.SetLocation.class, SET_LOCATION)
          .put(MetadataUpdate.AddViewVersion.class, ADD_VIEW_VERSION)
          .put(MetadataUpdate.SetCurrentViewVersion.class, SET_CURRENT_VIEW_VERSION)
          .put(MetadataUpdate.RemovePartitionSpecs.class, REMOVE_PARTITION_SPECS)
          .put(MetadataUpdate.RemoveSchemas.class, REMOVE_SCHEMAS)
          .put(MetadataUpdate.AddEncryptionKey.class, ADD_ENCRYPTION_KEY)
          .put(MetadataUpdate.RemoveEncryptionKey.class, REMOVE_ENCRYPTION_KEY)
          .put(MetadataUpdate.ProduceSnapshotUpdate.class, PRODUCE_SNAPSHOT)
          .buildOrThrow();

  public static String toJson(MetadataUpdate metadataUpdate) {
    return toJson(metadataUpdate, false);
  }

  public static String toJson(MetadataUpdate metadataUpdate, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(metadataUpdate, gen), pretty);
  }

  /**
   * Converts a MetadataUpdate to JSON with partition spec context.
   *
   * <p>This overload is required for serializing resolved {@link
   * MetadataUpdate.ProduceSnapshotUpdate} objects that contain DataFile/DeleteFile instances. The
   * partition specs are needed to serialize the partition data correctly.
   *
   * @param metadataUpdate the update to serialize
   * @param specsById partition specs by ID
   * @return JSON string representation
   */
  public static String toJson(MetadataUpdate metadataUpdate, Map<Integer, PartitionSpec> specsById) {
    return toJson(metadataUpdate, specsById, false);
  }

  /**
   * Converts a MetadataUpdate to JSON with partition spec context.
   *
   * @param metadataUpdate the update to serialize
   * @param specsById partition specs by ID
   * @param pretty whether to format the output
   * @return JSON string representation
   */
  public static String toJson(
      MetadataUpdate metadataUpdate, Map<Integer, PartitionSpec> specsById, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(metadataUpdate, specsById, gen), pretty);
  }

  /**
   * Writes a MetadataUpdate to JSON with partition spec context.
   *
   * @param metadataUpdate the update to serialize
   * @param specsById partition specs by ID
   * @param generator the JSON generator
   * @throws IOException if serialization fails
   */
  public static void toJson(
      MetadataUpdate metadataUpdate, Map<Integer, PartitionSpec> specsById, JsonGenerator generator)
      throws IOException {
    String updateAction = ACTIONS.get(metadataUpdate.getClass());
    Preconditions.checkArgument(
        updateAction != null,
        "Cannot convert metadata update to json. Unrecognized metadata update type: %s",
        metadataUpdate.getClass().getName());

    generator.writeStartObject();
    generator.writeStringField(ACTION, updateAction);

    if (PRODUCE_SNAPSHOT.equals(updateAction)) {
      writeProduceSnapshotUpdate(
          (MetadataUpdate.ProduceSnapshotUpdate) metadataUpdate, specsById, generator);
    } else {
      // Delegate to standard toJson for non-file updates (specsById not needed)
      writeUpdateContent(metadataUpdate, updateAction, generator);
    }

    generator.writeEndObject();
  }

  public static void toJson(MetadataUpdate metadataUpdate, JsonGenerator generator)
      throws IOException {
    String updateAction = ACTIONS.get(metadataUpdate.getClass());

    // Provide better exception message than the NPE thrown by writing null for the update action,
    // which is required
    Preconditions.checkArgument(
        updateAction != null,
        "Cannot convert metadata update to json. Unrecognized metadata update type: %s",
        metadataUpdate.getClass().getName());

    generator.writeStartObject();
    generator.writeStringField(ACTION, updateAction);

    writeUpdateContent(metadataUpdate, updateAction, generator);

    generator.writeEndObject();
  }

  private static void writeUpdateContent(
      MetadataUpdate metadataUpdate, String updateAction, JsonGenerator generator)
      throws IOException {
    switch (updateAction) {
      case ASSIGN_UUID:
        writeAssignUUID((MetadataUpdate.AssignUUID) metadataUpdate, generator);
        break;
      case UPGRADE_FORMAT_VERSION:
        writeUpgradeFormatVersion((MetadataUpdate.UpgradeFormatVersion) metadataUpdate, generator);
        break;
      case ADD_SCHEMA:
        writeAddSchema((MetadataUpdate.AddSchema) metadataUpdate, generator);
        break;
      case SET_CURRENT_SCHEMA:
        writeSetCurrentSchema((MetadataUpdate.SetCurrentSchema) metadataUpdate, generator);
        break;
      case ADD_PARTITION_SPEC:
        writeAddPartitionSpec((MetadataUpdate.AddPartitionSpec) metadataUpdate, generator);
        break;
      case SET_DEFAULT_PARTITION_SPEC:
        writeSetDefaultPartitionSpec(
            (MetadataUpdate.SetDefaultPartitionSpec) metadataUpdate, generator);
        break;
      case ADD_SORT_ORDER:
        writeAddSortOrder((MetadataUpdate.AddSortOrder) metadataUpdate, generator);
        break;
      case SET_DEFAULT_SORT_ORDER:
        writeSetDefaultSortOrder((MetadataUpdate.SetDefaultSortOrder) metadataUpdate, generator);
        break;
      case SET_STATISTICS:
        writeSetStatistics((MetadataUpdate.SetStatistics) metadataUpdate, generator);
        break;
      case REMOVE_STATISTICS:
        writeRemoveStatistics((MetadataUpdate.RemoveStatistics) metadataUpdate, generator);
        break;
      case SET_PARTITION_STATISTICS:
        writeSetPartitionStatistics(
            (MetadataUpdate.SetPartitionStatistics) metadataUpdate, generator);
        break;
      case REMOVE_PARTITION_STATISTICS:
        writeRemovePartitionStatistics(
            (MetadataUpdate.RemovePartitionStatistics) metadataUpdate, generator);
        break;
      case ADD_SNAPSHOT:
        writeAddSnapshot((MetadataUpdate.AddSnapshot) metadataUpdate, generator);
        break;
      case REMOVE_SNAPSHOTS:
        writeRemoveSnapshots((MetadataUpdate.RemoveSnapshots) metadataUpdate, generator);
        break;
      case REMOVE_SNAPSHOT_REF:
        writeRemoveSnapshotRef((MetadataUpdate.RemoveSnapshotRef) metadataUpdate, generator);
        break;
      case SET_SNAPSHOT_REF:
        writeSetSnapshotRef((MetadataUpdate.SetSnapshotRef) metadataUpdate, generator);
        break;
      case SET_PROPERTIES:
        writeSetProperties((MetadataUpdate.SetProperties) metadataUpdate, generator);
        break;
      case REMOVE_PROPERTIES:
        writeRemoveProperties((MetadataUpdate.RemoveProperties) metadataUpdate, generator);
        break;
      case SET_LOCATION:
        writeSetLocation((MetadataUpdate.SetLocation) metadataUpdate, generator);
        break;
      case ADD_VIEW_VERSION:
        writeAddViewVersion((MetadataUpdate.AddViewVersion) metadataUpdate, generator);
        break;
      case SET_CURRENT_VIEW_VERSION:
        writeSetCurrentViewVersionId(
            (MetadataUpdate.SetCurrentViewVersion) metadataUpdate, generator);
        break;
      case REMOVE_PARTITION_SPECS:
        writeRemovePartitionSpecs((MetadataUpdate.RemovePartitionSpecs) metadataUpdate, generator);
        break;
      case REMOVE_SCHEMAS:
        writeRemoveSchemas((MetadataUpdate.RemoveSchemas) metadataUpdate, generator);
        break;
      case ADD_ENCRYPTION_KEY:
        writeAddEncryptionKey((MetadataUpdate.AddEncryptionKey) metadataUpdate, generator);
        break;
      case REMOVE_ENCRYPTION_KEY:
        writeRemoveEncryptionKey((MetadataUpdate.RemoveEncryptionKey) metadataUpdate, generator);
        break;
      case PRODUCE_SNAPSHOT:
        writeProduceSnapshotUpdate(
            (MetadataUpdate.ProduceSnapshotUpdate) metadataUpdate, generator);
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert metadata update to json. Unrecognized action: %s", updateAction));
    }
  }

  /**
   * Read MetadataUpdate from a JSON string.
   *
   * @param json a JSON string of a MetadataUpdate
   * @return a MetadataUpdate object
   */
  public static MetadataUpdate fromJson(String json) {
    return JsonUtil.parse(json, MetadataUpdateParser::fromJson);
  }

  public static MetadataUpdate fromJson(JsonNode jsonNode) {
    Preconditions.checkArgument(
        jsonNode != null && jsonNode.isObject(),
        "Cannot parse metadata update from non-object value: %s",
        jsonNode);
    Preconditions.checkArgument(
        jsonNode.hasNonNull(ACTION), "Cannot parse metadata update. Missing field: action");
    String action = JsonUtil.getString(ACTION, jsonNode).toLowerCase(Locale.ROOT);

    switch (action) {
      case ASSIGN_UUID:
        return readAssignUUID(jsonNode);
      case UPGRADE_FORMAT_VERSION:
        return readUpgradeFormatVersion(jsonNode);
      case ADD_SCHEMA:
        return readAddSchema(jsonNode);
      case SET_CURRENT_SCHEMA:
        return readSetCurrentSchema(jsonNode);
      case ADD_PARTITION_SPEC:
        return readAddPartitionSpec(jsonNode);
      case SET_DEFAULT_PARTITION_SPEC:
        return readSetDefaultPartitionSpec(jsonNode);
      case ADD_SORT_ORDER:
        return readAddSortOrder(jsonNode);
      case SET_DEFAULT_SORT_ORDER:
        return readSetDefaultSortOrder(jsonNode);
      case SET_STATISTICS:
        return readSetStatistics(jsonNode);
      case REMOVE_STATISTICS:
        return readRemoveStatistics(jsonNode);
      case SET_PARTITION_STATISTICS:
        return readSetPartitionStatistics(jsonNode);
      case REMOVE_PARTITION_STATISTICS:
        return readRemovePartitionStatistics(jsonNode);
      case ADD_SNAPSHOT:
        return readAddSnapshot(jsonNode);
      case REMOVE_SNAPSHOTS:
        return readRemoveSnapshots(jsonNode);
      case REMOVE_SNAPSHOT_REF:
        return readRemoveSnapshotRef(jsonNode);
      case SET_SNAPSHOT_REF:
        return readSetSnapshotRef(jsonNode);
      case SET_PROPERTIES:
        return readSetProperties(jsonNode);
      case REMOVE_PROPERTIES:
        return readRemoveProperties(jsonNode);
      case SET_LOCATION:
        return readSetLocation(jsonNode);
      case ADD_VIEW_VERSION:
        return readAddViewVersion(jsonNode);
      case SET_CURRENT_VIEW_VERSION:
        return readCurrentViewVersionId(jsonNode);
      case REMOVE_PARTITION_SPECS:
        return readRemovePartitionSpecs(jsonNode);
      case REMOVE_SCHEMAS:
        return readRemoveSchemas(jsonNode);
      case ADD_ENCRYPTION_KEY:
        return readAddEncryptionKey(jsonNode);
      case REMOVE_ENCRYPTION_KEY:
        return readRemoveEncryptionKey(jsonNode);
      case PRODUCE_SNAPSHOT:
        return readProduceSnapshotUpdate(jsonNode);
      default:
        throw new UnsupportedOperationException(
            String.format("Cannot convert metadata update action to json: %s", action));
    }
  }

  private static void writeAssignUUID(MetadataUpdate.AssignUUID update, JsonGenerator gen)
      throws IOException {
    gen.writeStringField(UUID, update.uuid());
  }

  private static void writeUpgradeFormatVersion(
      MetadataUpdate.UpgradeFormatVersion update, JsonGenerator gen) throws IOException {
    gen.writeNumberField(FORMAT_VERSION, update.formatVersion());
  }

  private static void writeAddSchema(MetadataUpdate.AddSchema update, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(SCHEMA);
    SchemaParser.toJson(update.schema(), gen);
    gen.writeNumberField(LAST_COLUMN_ID, update.lastColumnId());
  }

  private static void writeSetCurrentSchema(
      MetadataUpdate.SetCurrentSchema update, JsonGenerator gen) throws IOException {
    gen.writeNumberField(SCHEMA_ID, update.schemaId());
  }

  private static void writeAddPartitionSpec(
      MetadataUpdate.AddPartitionSpec update, JsonGenerator gen) throws IOException {
    gen.writeFieldName(SPEC);
    PartitionSpecParser.toJson(update.spec(), gen);
  }

  private static void writeSetDefaultPartitionSpec(
      MetadataUpdate.SetDefaultPartitionSpec update, JsonGenerator gen) throws IOException {
    gen.writeNumberField(SPEC_ID, update.specId());
  }

  private static void writeAddSortOrder(MetadataUpdate.AddSortOrder update, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(SORT_ORDER);
    SortOrderParser.toJson(update.sortOrder(), gen);
  }

  private static void writeSetDefaultSortOrder(
      MetadataUpdate.SetDefaultSortOrder update, JsonGenerator gen) throws IOException {
    gen.writeNumberField(SORT_ORDER_ID, update.sortOrderId());
  }

  private static void writeSetStatistics(MetadataUpdate.SetStatistics update, JsonGenerator gen)
      throws IOException {
    gen.writeNumberField(SNAPSHOT_ID, update.snapshotId());
    gen.writeFieldName(STATISTICS);
    StatisticsFileParser.toJson(update.statisticsFile(), gen);
  }

  private static void writeRemoveStatistics(
      MetadataUpdate.RemoveStatistics update, JsonGenerator gen) throws IOException {
    gen.writeNumberField(SNAPSHOT_ID, update.snapshotId());
  }

  private static void writeSetPartitionStatistics(
      MetadataUpdate.SetPartitionStatistics update, JsonGenerator gen) throws IOException {
    gen.writeFieldName(PARTITION_STATISTICS);
    PartitionStatisticsFileParser.toJson(update.partitionStatisticsFile(), gen);
  }

  private static void writeRemovePartitionStatistics(
      MetadataUpdate.RemovePartitionStatistics update, JsonGenerator gen) throws IOException {
    gen.writeNumberField(SNAPSHOT_ID, update.snapshotId());
  }

  private static void writeAddSnapshot(MetadataUpdate.AddSnapshot update, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(SNAPSHOT);
    SnapshotParser.toJson(update.snapshot(), gen);
  }

  private static void writeRemoveSnapshots(MetadataUpdate.RemoveSnapshots update, JsonGenerator gen)
      throws IOException {
    JsonUtil.writeLongArray(SNAPSHOT_IDS, update.snapshotIds(), gen);
  }

  private static void writeSetSnapshotRef(MetadataUpdate.SetSnapshotRef update, JsonGenerator gen)
      throws IOException {
    gen.writeStringField(REF_NAME, update.name());
    gen.writeNumberField(SNAPSHOT_ID, update.snapshotId());
    gen.writeStringField(TYPE, update.type());
    JsonUtil.writeIntegerFieldIf(
        update.minSnapshotsToKeep() != null,
        MIN_SNAPSHOTS_TO_KEEP,
        update.minSnapshotsToKeep(),
        gen);
    JsonUtil.writeLongFieldIf(
        update.maxSnapshotAgeMs() != null, MAX_SNAPSHOT_AGE_MS, update.maxSnapshotAgeMs(), gen);
    JsonUtil.writeLongFieldIf(
        update.maxRefAgeMs() != null, MAX_REF_AGE_MS, update.maxRefAgeMs(), gen);
  }

  private static void writeRemoveSnapshotRef(
      MetadataUpdate.RemoveSnapshotRef update, JsonGenerator gen) throws IOException {
    gen.writeStringField(REF_NAME, update.name());
  }

  private static void writeSetProperties(MetadataUpdate.SetProperties update, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(UPDATES);
    gen.writeObject(update.updated());
  }

  private static void writeRemoveProperties(
      MetadataUpdate.RemoveProperties update, JsonGenerator gen) throws IOException {
    gen.writeFieldName(REMOVALS);
    gen.writeObject(update.removed());
  }

  private static void writeSetLocation(MetadataUpdate.SetLocation update, JsonGenerator gen)
      throws IOException {
    gen.writeStringField(LOCATION, update.location());
  }

  private static void writeAddViewVersion(
      MetadataUpdate.AddViewVersion metadataUpdate, JsonGenerator gen) throws IOException {
    gen.writeFieldName(VIEW_VERSION);
    ViewVersionParser.toJson(metadataUpdate.viewVersion(), gen);
  }

  private static void writeSetCurrentViewVersionId(
      MetadataUpdate.SetCurrentViewVersion metadataUpdate, JsonGenerator gen) throws IOException {
    gen.writeNumberField(VIEW_VERSION_ID, metadataUpdate.versionId());
  }

  private static void writeRemovePartitionSpecs(
      MetadataUpdate.RemovePartitionSpecs metadataUpdate, JsonGenerator gen) throws IOException {
    JsonUtil.writeIntegerArray(SPEC_IDS, metadataUpdate.specIds(), gen);
  }

  private static void writeRemoveSchemas(
      MetadataUpdate.RemoveSchemas metadataUpdate, JsonGenerator gen) throws IOException {
    JsonUtil.writeIntegerArray(SCHEMA_IDS, metadataUpdate.schemaIds(), gen);
  }

  private static void writeAddEncryptionKey(
      MetadataUpdate.AddEncryptionKey update, JsonGenerator gen) throws IOException {
    gen.writeFieldName(ENCRYPTION_KEY);
    EncryptedKeyParser.toJson(update.key(), gen);
  }

  private static void writeRemoveEncryptionKey(
      MetadataUpdate.RemoveEncryptionKey update, JsonGenerator gen) throws IOException {
    gen.writeStringField(KEY_ID, update.keyId());
  }

  private static MetadataUpdate readAssignUUID(JsonNode node) {
    String uuid = JsonUtil.getString(UUID, node);
    return new MetadataUpdate.AssignUUID(uuid);
  }

  private static MetadataUpdate readUpgradeFormatVersion(JsonNode node) {
    int formatVersion = JsonUtil.getInt(FORMAT_VERSION, node);
    return new MetadataUpdate.UpgradeFormatVersion(formatVersion);
  }

  private static MetadataUpdate readAddSchema(JsonNode node) {
    JsonNode schemaNode = JsonUtil.get(SCHEMA, node);
    Schema schema = SchemaParser.fromJson(schemaNode);
    return new MetadataUpdate.AddSchema(schema);
  }

  private static MetadataUpdate readSetCurrentSchema(JsonNode node) {
    int schemaId = JsonUtil.getInt(SCHEMA_ID, node);
    return new MetadataUpdate.SetCurrentSchema(schemaId);
  }

  private static MetadataUpdate readAddPartitionSpec(JsonNode node) {
    JsonNode specNode = JsonUtil.get(SPEC, node);
    UnboundPartitionSpec spec = PartitionSpecParser.fromJson(specNode);
    return new MetadataUpdate.AddPartitionSpec(spec);
  }

  private static MetadataUpdate readSetDefaultPartitionSpec(JsonNode node) {
    int specId = JsonUtil.getInt(SPEC_ID, node);
    return new MetadataUpdate.SetDefaultPartitionSpec(specId);
  }

  private static MetadataUpdate readAddSortOrder(JsonNode node) {
    JsonNode sortOrderNode = JsonUtil.get(SORT_ORDER, node);
    UnboundSortOrder sortOrder = SortOrderParser.fromJson(sortOrderNode);
    return new MetadataUpdate.AddSortOrder(sortOrder);
  }

  private static MetadataUpdate readSetDefaultSortOrder(JsonNode node) {
    int sortOrderId = JsonUtil.getInt(SORT_ORDER_ID, node);
    return new MetadataUpdate.SetDefaultSortOrder(sortOrderId);
  }

  private static MetadataUpdate readSetStatistics(JsonNode node) {
    JsonNode statisticsFileNode = JsonUtil.get(STATISTICS, node);
    StatisticsFile statisticsFile = StatisticsFileParser.fromJson(statisticsFileNode);
    return new MetadataUpdate.SetStatistics(statisticsFile);
  }

  private static MetadataUpdate readRemoveStatistics(JsonNode node) {
    long snapshotId = JsonUtil.getLong(SNAPSHOT_ID, node);
    return new MetadataUpdate.RemoveStatistics(snapshotId);
  }

  private static MetadataUpdate readSetPartitionStatistics(JsonNode node) {
    JsonNode partitionStatisticsFileNode = JsonUtil.get(PARTITION_STATISTICS, node);
    PartitionStatisticsFile partitionStatisticsFile =
        PartitionStatisticsFileParser.fromJson(partitionStatisticsFileNode);
    return new MetadataUpdate.SetPartitionStatistics(partitionStatisticsFile);
  }

  private static MetadataUpdate readRemovePartitionStatistics(JsonNode node) {
    long snapshotId = JsonUtil.getLong(SNAPSHOT_ID, node);
    return new MetadataUpdate.RemovePartitionStatistics(snapshotId);
  }

  private static MetadataUpdate readAddSnapshot(JsonNode node) {
    Snapshot snapshot = SnapshotParser.fromJson(JsonUtil.get(SNAPSHOT, node));
    return new MetadataUpdate.AddSnapshot(snapshot);
  }

  private static MetadataUpdate readRemoveSnapshots(JsonNode node) {
    Set<Long> snapshotIds = JsonUtil.getLongSetOrNull(SNAPSHOT_IDS, node);
    Preconditions.checkArgument(
        snapshotIds != null,
        "Invalid set of snapshot ids to remove: must be non-null",
        snapshotIds);
    return new MetadataUpdate.RemoveSnapshots(snapshotIds);
  }

  private static MetadataUpdate readSetSnapshotRef(JsonNode node) {
    String refName = JsonUtil.getString(REF_NAME, node);
    long snapshotId = JsonUtil.getLong(SNAPSHOT_ID, node);
    SnapshotRefType type = SnapshotRefType.fromString(JsonUtil.getString(TYPE, node));
    Integer minSnapshotsToKeep = JsonUtil.getIntOrNull(MIN_SNAPSHOTS_TO_KEEP, node);
    Long maxSnapshotAgeMs = JsonUtil.getLongOrNull(MAX_SNAPSHOT_AGE_MS, node);
    Long maxRefAgeMs = JsonUtil.getLongOrNull(MAX_REF_AGE_MS, node);
    return new MetadataUpdate.SetSnapshotRef(
        refName, snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs);
  }

  private static MetadataUpdate readRemoveSnapshotRef(JsonNode node) {
    String refName = JsonUtil.getString(REF_NAME, node);
    return new MetadataUpdate.RemoveSnapshotRef(refName);
  }

  private static MetadataUpdate readSetProperties(JsonNode node) {
    Map<String, String> updates;

    boolean hasLegacyField = node.has(UPDATED);
    boolean hasUpdatesField = node.has(UPDATES);
    if (hasLegacyField && hasUpdatesField) {
      updates = JsonUtil.getStringMap(UPDATES, node);
    } else if (hasLegacyField) {
      updates = JsonUtil.getStringMap(UPDATED, node);
    } else {
      updates = JsonUtil.getStringMap(UPDATES, node);
    }

    return new MetadataUpdate.SetProperties(updates);
  }

  private static MetadataUpdate readRemoveProperties(JsonNode node) {
    Set<String> removals;

    boolean hasLegacyField = node.has(REMOVED);
    boolean hasRemovalsField = node.has(REMOVALS);
    if (hasLegacyField && hasRemovalsField) {
      removals = JsonUtil.getStringSet(REMOVALS, node);
    } else if (hasLegacyField) {
      removals = JsonUtil.getStringSet(REMOVED, node);
    } else {
      removals = JsonUtil.getStringSet(REMOVALS, node);
    }

    return new MetadataUpdate.RemoveProperties(removals);
  }

  private static MetadataUpdate readSetLocation(JsonNode node) {
    String location = JsonUtil.getString(LOCATION, node);
    return new MetadataUpdate.SetLocation(location);
  }

  private static MetadataUpdate readAddViewVersion(JsonNode node) {
    return new MetadataUpdate.AddViewVersion(
        ViewVersionParser.fromJson(JsonUtil.get(VIEW_VERSION, node)));
  }

  private static MetadataUpdate readCurrentViewVersionId(JsonNode node) {
    return new MetadataUpdate.SetCurrentViewVersion(JsonUtil.getInt(VIEW_VERSION_ID, node));
  }

  private static MetadataUpdate readRemovePartitionSpecs(JsonNode node) {
    return new MetadataUpdate.RemovePartitionSpecs(JsonUtil.getIntegerSet(SPEC_IDS, node));
  }

  private static MetadataUpdate readRemoveSchemas(JsonNode node) {
    return new MetadataUpdate.RemoveSchemas(JsonUtil.getIntegerSet(SCHEMA_IDS, node));
  }

  private static MetadataUpdate readAddEncryptionKey(JsonNode node) {
    JsonNode keyNode = node.get(ENCRYPTION_KEY);
    Preconditions.checkArgument(
        keyNode != null && keyNode.isObject(),
        "Invalid encryption key, must be non-null object: %s",
        keyNode);
    return new MetadataUpdate.AddEncryptionKey(EncryptedKeyParser.fromJson(keyNode));
  }

  private static MetadataUpdate readRemoveEncryptionKey(JsonNode node) {
    String keyId = JsonUtil.getString(KEY_ID, node);
    return new MetadataUpdate.RemoveEncryptionKey(keyId);
  }

  private static void writeProduceSnapshotUpdate(
      MetadataUpdate.ProduceSnapshotUpdate update, JsonGenerator gen) throws IOException {
    writeProduceSnapshotUpdate(update, null, gen);
  }

  /**
   * Writes a ProduceSnapshotUpdate to JSON.
   *
   * @param update the update to serialize
   * @param specsById partition specs by ID, required for resolved updates
   * @param gen the JSON generator
   * @throws IOException if serialization fails
   * @throws IllegalStateException if update is resolved but specsById is not provided
   */
  static void writeProduceSnapshotUpdate(
      MetadataUpdate.ProduceSnapshotUpdate update,
      Map<Integer, PartitionSpec> specsById,
      JsonGenerator gen)
      throws IOException {
    gen.writeStringField(SNAPSHOT_ACTION, update.action());

    if (update.isUnresolved()) {
      // Unresolved: re-serialize the JSON nodes
      writeJsonNodeArray(ADD_DATA_FILES, update.unresolvedAddDataFiles(), gen);
      writeJsonNodeArray(ADD_DELETE_FILES, update.unresolvedAddDeleteFiles(), gen);
      writeJsonNodeArray(REMOVE_DATA_FILES, update.unresolvedRemoveDataFiles(), gen);
      writeJsonNodeArray(REMOVE_DELETE_FILES, update.unresolvedRemoveDeleteFiles(), gen);
    } else {
      // Resolved: serialize using ContentFileParser (requires specs)
      Preconditions.checkState(
          specsById != null,
          "Cannot serialize resolved ProduceSnapshotUpdate without partition specs. "
              + "Use toJson(update, specsById, generator) instead.");
      writeContentFileArray(ADD_DATA_FILES, update.addDataFiles(), specsById, gen);
      writeContentFileArray(ADD_DELETE_FILES, update.addDeleteFiles(), specsById, gen);
      writeContentFileArray(REMOVE_DATA_FILES, update.removeDataFiles(), specsById, gen);
      writeContentFileArray(REMOVE_DELETE_FILES, update.removeDeleteFiles(), specsById, gen);
    }

    if (update.deleteRowFilter() != null) {
      gen.writeFieldName(DELETE_ROW_FILTER);
      ExpressionParser.toJson(update.deleteRowFilter(), gen);
    }

    gen.writeBooleanField(STAGE_ONLY, update.stageOnly());

    if (update.branch() != null) {
      gen.writeStringField(BRANCH, update.branch());
    }

    if (update.summary() != null && !update.summary().isEmpty()) {
      JsonUtil.writeStringMap(SUMMARY, update.summary(), gen);
    }

    if (update.baseSnapshotId() != null) {
      gen.writeNumberField(BASE_SNAPSHOT_ID, update.baseSnapshotId());
    }

    if (update.validations() != null && !update.validations().isEmpty()) {
      gen.writeArrayFieldStart(COMMIT_VALIDATIONS);
      for (org.apache.iceberg.rest.CommitValidation validation : update.validations()) {
        org.apache.iceberg.rest.CommitValidationParser.toJson(validation, gen);
      }
      gen.writeEndArray();
    }
  }

  private static void writeJsonNodeArray(
      String fieldName, List<JsonNode> nodes, JsonGenerator gen) throws IOException {
    if (nodes != null && !nodes.isEmpty()) {
      gen.writeArrayFieldStart(fieldName);
      for (JsonNode node : nodes) {
        gen.writeTree(node);
      }
      gen.writeEndArray();
    }
  }

  private static <T extends ContentFile<?>> void writeContentFileArray(
      String fieldName,
      List<T> files,
      Map<Integer, PartitionSpec> specsById,
      JsonGenerator gen)
      throws IOException {
    if (files != null && !files.isEmpty()) {
      gen.writeArrayFieldStart(fieldName);
      for (T file : files) {
        PartitionSpec spec = specsById.get(file.specId());
        Preconditions.checkArgument(
            spec != null, "Cannot find partition spec for spec-id: %s", file.specId());
        ContentFileParser.toJson(file, spec, gen);
      }
      gen.writeEndArray();
    }
  }

  private static MetadataUpdate readProduceSnapshotUpdate(JsonNode node) {
    Preconditions.checkArgument(
        node.hasNonNull(SNAPSHOT_ACTION), "Missing required field: %s", SNAPSHOT_ACTION);

    String action = JsonUtil.getString(SNAPSHOT_ACTION, node);

    List<JsonNode> addDataFiles = readJsonNodeArray(ADD_DATA_FILES, node);
    List<JsonNode> addDeleteFiles = readJsonNodeArray(ADD_DELETE_FILES, node);
    List<JsonNode> removeDataFiles = readJsonNodeArray(REMOVE_DATA_FILES, node);
    List<JsonNode> removeDeleteFiles = readJsonNodeArray(REMOVE_DELETE_FILES, node);

    Expression deleteRowFilter = null;
    if (node.hasNonNull(DELETE_ROW_FILTER)) {
      deleteRowFilter = ExpressionParser.fromJson(node.get(DELETE_ROW_FILTER));
    }

    Boolean stageOnlyValue = JsonUtil.getBoolOrNull(STAGE_ONLY, node);
    boolean stageOnly = stageOnlyValue != null ? stageOnlyValue : false;

    String branch = JsonUtil.getStringOrNull(BRANCH, node);

    Map<String, String> summary = null;
    if (node.hasNonNull(SUMMARY)) {
      summary = JsonUtil.getStringMap(SUMMARY, node);
    }

    Long baseSnapshotId = JsonUtil.getLongOrNull(BASE_SNAPSHOT_ID, node);

    List<org.apache.iceberg.rest.CommitValidation> validations = null;
    if (node.hasNonNull(COMMIT_VALIDATIONS)) {
      validations =
          org.apache.iceberg.relocated.com.google.common.collect.Lists.newArrayList();
      for (JsonNode validationNode : node.get(COMMIT_VALIDATIONS)) {
        validations.add(org.apache.iceberg.rest.CommitValidationParser.fromJson(validationNode));
      }
    }

    return MetadataUpdate.ProduceSnapshotUpdate.createUnresolved(
        action,
        addDataFiles,
        addDeleteFiles,
        removeDataFiles,
        removeDeleteFiles,
        deleteRowFilter,
        stageOnly,
        branch,
        summary,
        baseSnapshotId,
        validations);
  }

  private static List<JsonNode> readJsonNodeArray(String fieldName, JsonNode parentNode) {
    if (!parentNode.hasNonNull(fieldName)) {
      return null;
    }

    JsonNode arrayNode = parentNode.get(fieldName);
    Preconditions.checkArgument(
        arrayNode.isArray(), "Expected array for field %s, got: %s", fieldName, arrayNode);

    List<JsonNode> result = new ArrayList<>(arrayNode.size());
    Iterator<JsonNode> elements = arrayNode.elements();
    while (elements.hasNext()) {
      result.add(elements.next());
    }
    return result;
  }
}
