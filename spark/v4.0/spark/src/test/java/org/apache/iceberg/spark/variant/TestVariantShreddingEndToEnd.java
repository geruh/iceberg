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
package org.apache.iceberg.spark.variant;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantObject;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end tests for variant shredding that demonstrate: 1. How shredded columns appear in
 * Parquet files 2. How Iceberg collects and stores variant bounds in manifests 3. How those bounds
 * could enable file pruning (when filter pushdown is implemented)
 */
public class TestVariantShreddingEndToEnd extends CatalogTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestVariantShreddingEndToEnd.class);

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.VariantType.get()));

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties()
      },
    };
  }

  @BeforeAll
  public static void startMetastoreAndSpark() {
    CatalogTestBase.startMetastoreAndSpark();
    if (spark != null) {
      spark.stop();
    }
    spark =
        SparkSession.builder()
            .master("local[1]")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
            .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
            .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
            .enableHiveSupport()
            .getOrCreate();
    sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  @BeforeEach
  public void before() {
    super.before();
    validationCatalog.createTable(
        tableIdent,
        SCHEMA,
        null,
        Map.of(
            TableProperties.FORMAT_VERSION, "3",
            TableProperties.DEFAULT_WRITE_METRICS_MODE, "full"));
  }

  @AfterEach
  public void after() {
    validationCatalog.dropTable(tableIdent, true);
  }

  /**
   * This test demonstrates the complete variant shredding flow: 1. Write data with shredding
   * enabled 2. Examine the Parquet file structure to see shredded columns 3. Examine Iceberg
   * manifest bounds to see how min/max are tracked 4. Show how bounds could enable file pruning
   */
  @TestTemplate
  public void testShreddedWriteWithMetadataInspection() throws IOException {
    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");

    // Write data with different numeric values to demonstrate bounds tracking
    String values =
        "(1, parse_json('{\"name\": \"Alice\", \"age\": 25, \"score\": 85.5}')), "
            + "(2, parse_json('{\"name\": \"Bob\", \"age\": 30, \"score\": 92.0}')), "
            + "(3, parse_json('{\"name\": \"Charlie\", \"age\": 35, \"score\": 78.5}'))";
    sql("INSERT INTO %s VALUES %s", tableName, values);

    Table table = validationCatalog.loadTable(tableIdent);

    // === PART 1: Examine Parquet File Structure ===
    LOG.info("=== PARQUET FILE STRUCTURE ===");
    try (CloseableIterable<FileScanTask> tasks = table.newScan().includeColumnStats().planFiles()) {
      for (FileScanTask task : tasks) {
        String path = task.file().location();
        LOG.info("File: {}", path);

        HadoopInputFile inputFile =
            HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path), new Configuration());

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
          MessageType schema = reader.getFileMetaData().getSchema();
          LOG.info("Parquet Schema:");
          printParquetSchema(schema, "  ");

          // Verify the data column is shredded (has typed_value)
          Type dataField = schema.getType("data");
          assertThat(dataField).isInstanceOf(GroupType.class);
          GroupType dataGroup = (GroupType) dataField;

          // Should have: metadata, value, typed_value
          assertThat(dataGroup.containsField("metadata")).isTrue();
          assertThat(dataGroup.containsField("value")).isTrue();
          assertThat(dataGroup.containsField("typed_value")).isTrue();

          LOG.info("Verified: 'data' column is shredded with typed_value");
        }
      }
    }

    // === PART 2: Examine Iceberg Manifest Bounds ===
    LOG.info("\n=== ICEBERG MANIFEST BOUNDS ===");
    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    for (ManifestFile manifest : manifests) {
      LOG.info("Manifest: {}", manifest.path());
    }

    try (CloseableIterable<FileScanTask> tasks = table.newScan().includeColumnStats().planFiles()) {
      for (FileScanTask task : tasks) {
        DataFile dataFile = task.file();
        LOG.info("\nDataFile: {}", dataFile.location());
        LOG.info("  Record count: {}", dataFile.recordCount());
        LOG.info("  File size: {} bytes", dataFile.fileSizeInBytes());

        // Examine lower bounds
        Map<Integer, ByteBuffer> lowerBounds = dataFile.lowerBounds();
        Map<Integer, ByteBuffer> upperBounds = dataFile.upperBounds();

        LOG.info("\n  Lower Bounds:");
        if (lowerBounds != null) {
          for (Map.Entry<Integer, ByteBuffer> entry : lowerBounds.entrySet()) {
            int fieldId = entry.getKey();
            ByteBuffer buffer = entry.getValue().duplicate();

            if (fieldId == 2) { // data column (variant)
              // Try to parse as Variant format first, fall back to raw bytes
              try {
                // Variant format uses little-endian byte order
                ByteBuffer leBuffer = buffer.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN);
                Variant variantBounds = Variant.from(leBuffer);
                VariantObject boundsObj = variantBounds.value().asObject();
                LOG.info(
                    "    Field {} (data/variant): {}", fieldId, formatVariantBounds(boundsObj));
              } catch (Exception e) {
                // Not in Variant format - log raw bytes
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                LOG.info(
                    "    Field {} (data/variant): raw bytes (not Variant format): {} bytes",
                    fieldId,
                    bytes.length);
                LOG.info("      First bytes: {}", bytesToHex(bytes, Math.min(16, bytes.length)));
                LOG.info("      Parse error: {}", e.getMessage());
              }
            } else {
              LOG.info("    Field {} (id): {}", fieldId, buffer.getInt(0));
            }
          }
        }

        LOG.info("\n  Upper Bounds:");
        if (upperBounds != null) {
          for (Map.Entry<Integer, ByteBuffer> entry : upperBounds.entrySet()) {
            int fieldId = entry.getKey();
            ByteBuffer buffer = entry.getValue().duplicate();

            if (fieldId == 2) { // data column (variant)
              try {
                ByteBuffer leBuffer = buffer.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN);
                Variant variantBounds = Variant.from(leBuffer);
                VariantObject boundsObj = variantBounds.value().asObject();
                LOG.info(
                    "    Field {} (data/variant): {}", fieldId, formatVariantBounds(boundsObj));
              } catch (Exception e) {
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                LOG.info(
                    "    Field {} (data/variant): raw bytes (not Variant format): {} bytes",
                    fieldId,
                    bytes.length);
                LOG.info("      First bytes: {}", bytesToHex(bytes, Math.min(16, bytes.length)));
                LOG.info("      Parse error: {}", e.getMessage());
              }
            } else {
              LOG.info("    Field {} (id): {}", fieldId, buffer.getInt(0));
            }
          }
        }

        // Check if bounds exist for the variant column
        if (lowerBounds != null && lowerBounds.containsKey(2)) {
          LOG.info("\nBounds ARE present for variant column (field ID 2)");
        } else {
          LOG.info("\nNote: Variant bounds NOT present in manifest");
          LOG.info("  Lower bounds keys: {}", lowerBounds != null ? lowerBounds.keySet() : "null");
          LOG.info("  Upper bounds keys: {}", upperBounds != null ? upperBounds.keySet() : "null");
        }
      }
    }

    // === PART 3: Demonstrate Data Can Be Read Back ===
    LOG.info("\n=== DATA VERIFICATION ===");
    Dataset<Row> results =
        spark.sql(
            String.format(
                "SELECT id, "
                    + "try_variant_get(data, '$.name', 'string') as name, "
                    + "try_variant_get(data, '$.age', 'int') as age, "
                    + "try_variant_get(data, '$.score', 'double') as score "
                    + "FROM %s ORDER BY id",
                tableName));

    List<Row> rows = results.collectAsList();
    assertThat(rows).hasSize(3);
    assertThat(rows.get(0).getString(1)).isEqualTo("Alice");
    assertThat(rows.get(0).getInt(2)).isEqualTo(25);
    assertThat(rows.get(1).getString(1)).isEqualTo("Bob");
    assertThat(rows.get(1).getInt(2)).isEqualTo(30);
    assertThat(rows.get(2).getString(1)).isEqualTo("Charlie");
    assertThat(rows.get(2).getInt(2)).isEqualTo(35);

    LOG.info("All data read back correctly from shredded columns");
  }

  /**
   * Test that demonstrates bounds tracking across multiple files. When filter pushdown is
   * implemented, Iceberg can use these bounds to skip entire files that don't match predicates.
   */
  @TestTemplate
  public void testBoundsAcrossMultipleFiles() throws IOException {
    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");

    // Write first file with ages 20-30
    sql(
        "INSERT INTO %s VALUES "
            + "(1, parse_json('{\"age\": 20}')), "
            + "(2, parse_json('{\"age\": 25}')), "
            + "(3, parse_json('{\"age\": 30}'))",
        tableName);

    // Write second file with ages 40-50
    sql(
        "INSERT INTO %s VALUES "
            + "(4, parse_json('{\"age\": 40}')), "
            + "(5, parse_json('{\"age\": 45}')), "
            + "(6, parse_json('{\"age\": 50}'))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    LOG.info("=== BOUNDS ACROSS MULTIPLE FILES ===");
    LOG.info("This demonstrates how each file has its own bounds,");
    LOG.info("enabling file-level pruning when filter pushdown is implemented.\n");

    List<DataFile> dataFiles = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().includeColumnStats().planFiles()) {
      for (FileScanTask task : tasks) {
        dataFiles.add(task.file());
      }
    }

    assertThat(dataFiles).hasSize(2);

    for (int i = 0; i < dataFiles.size(); i++) {
      DataFile file = dataFiles.get(i);
      LOG.info("File {}: {}", i + 1, file.location());

      Map<Integer, ByteBuffer> lowerBounds = file.lowerBounds();
      Map<Integer, ByteBuffer> upperBounds = file.upperBounds();

      if (lowerBounds != null && upperBounds != null) {
        ByteBuffer lowerBound = lowerBounds.get(2);
        ByteBuffer upperBound = upperBounds.get(2);

        if (lowerBound != null && upperBound != null) {
          ByteBuffer lowerLE = lowerBound.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN);
          ByteBuffer upperLE = upperBound.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN);
          VariantObject lower = Variant.from(lowerLE).value().asObject();
          VariantObject upper = Variant.from(upperLE).value().asObject();
          LOG.info("  Lower bounds: {}", formatVariantBounds(lower));
          LOG.info("  Upper bounds: {}", formatVariantBounds(upper));
        } else {
          LOG.info("  Variant column bounds: NOT present");
          LOG.info("  Available field IDs with bounds: {}", lowerBounds.keySet());
        }
      } else {
        LOG.info("  No bounds collected for this file");
      }
    }

    LOG.info("\n=== HOW FILE PRUNING WOULD WORK ===");
    LOG.info("Query: WHERE try_variant_get(data, '$.age', 'int') > 35");
    LOG.info("Expected behavior (once filter pushdown is implemented):");
    LOG.info("  - File 1 (ages 20-30): SKIP (upper bound 30 < 35)");
    LOG.info("  - File 2 (ages 40-50): SCAN (bounds overlap with predicate)");
    LOG.info(
        "\nNote: Currently, both files are scanned because filter pushdown is not yet implemented.");

    // Verify both files would currently be scanned
    long rowsReturned =
        spark
            .sql(
                String.format(
                    "SELECT * FROM %s WHERE try_variant_get(data, '$.age', 'int') > 35", tableName))
            .count();
    assertThat(rowsReturned).isEqualTo(3); // ages 40, 45, 50
  }

  /**
   * Test partial shredding detection - when some values go to typed_value and some to value column,
   * bounds may be dropped for safety.
   */
  @TestTemplate
  public void testPartialShreddingBoundsHandling() throws IOException {
    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");

    // Write data where 'score' has type mismatch (int vs string)
    // This tests Iceberg's safety mechanism for partial shredding
    String values =
        "(1, parse_json('{\"score\": 100}')), " // int
            + "(2, parse_json('{\"score\": \"high\"}')), " // string - type mismatch!
            + "(3, parse_json('{\"score\": 200}'))"; // int
    sql("INSERT INTO %s VALUES %s", tableName, values);

    Table table = validationCatalog.loadTable(tableIdent);

    LOG.info("=== PARTIAL SHREDDING BOUNDS HANDLING ===");
    LOG.info("Data written: score=100 (int), score='high' (string), score=200 (int)");
    LOG.info("Expected: bounds for 'score' field should be DROPPED due to type mismatch\n");

    try (CloseableIterable<FileScanTask> tasks = table.newScan().includeColumnStats().planFiles()) {
      for (FileScanTask task : tasks) {
        DataFile file = task.file();
        Map<Integer, ByteBuffer> lowerBounds = file.lowerBounds();

        if (lowerBounds == null || !lowerBounds.containsKey(2)) {
          LOG.info("Variant bounds NOT collected during Spark write");
          LOG.info(
              "  Available bounds field IDs: {}",
              lowerBounds != null ? lowerBounds.keySet() : "null");
          continue;
        }

        ByteBuffer lowerBound = lowerBounds.get(2);
        if (lowerBound != null) {
          ByteBuffer lowerLE = lowerBound.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN);
          VariantObject lower = Variant.from(lowerLE).value().asObject();
          LOG.info("Lower bounds present: {}", formatVariantBounds(lower));

          // Check if 'score' field bounds were dropped
          // The path would be "$['score']" in the bounds object
          boolean hasScoreBounds = false;
          for (String key : lower.fieldNames()) {
            if (key.contains("score")) {
              hasScoreBounds = true;
              break;
            }
          }

          if (!hasScoreBounds) {
            LOG.info("Confirmed: 'score' bounds were correctly dropped due to type mismatch");
          } else {
            LOG.info("Note: 'score' bounds present (implementation may vary)");
          }
        }
      }
    }

    // Verify data can still be read correctly
    List<Row> results =
        spark
            .sql(
                String.format(
                    "SELECT try_variant_get(data, '$.score', 'string') FROM %s ORDER BY id",
                    tableName))
            .collectAsList();
    assertThat(results).hasSize(3);
    assertThat(results.get(0).getString(0)).isEqualTo("100"); // int serialized as string
    assertThat(results.get(1).getString(0)).isEqualTo("high"); // string
    assertThat(results.get(2).getString(0)).isEqualTo("200"); // int serialized as string
    LOG.info("Mixed type data read back correctly");
  }

  private void printParquetSchema(Type type, String indent) {
    if (type instanceof MessageType) {
      MessageType msg = (MessageType) type;
      LOG.info("{}message {} {{", indent, msg.getName());
      for (Type field : msg.getFields()) {
        printParquetSchema(field, indent + "  ");
      }
      LOG.info("{}}}", indent);
    } else if (type instanceof GroupType) {
      GroupType group = (GroupType) type;
      String annotation =
          group.getLogicalTypeAnnotation() != null
              ? " (" + group.getLogicalTypeAnnotation() + ")"
              : "";
      LOG.info(
          "{}{} group {}{} {{",
          indent,
          group.getRepetition().name().toLowerCase(),
          group.getName(),
          annotation);
      for (Type field : group.getFields()) {
        printParquetSchema(field, indent + "  ");
      }
      LOG.info("{}}}", indent);
    } else {
      String annotation =
          type.getLogicalTypeAnnotation() != null
              ? " (" + type.getLogicalTypeAnnotation() + ")"
              : "";
      LOG.info(
          "{}{} {} {}{};",
          indent,
          type.getRepetition().name().toLowerCase(),
          type.asPrimitiveType().getPrimitiveTypeName().name(),
          type.getName(),
          annotation);
    }
  }

  private String formatVariantBounds(VariantObject bounds) {
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (String fieldName : bounds.fieldNames()) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(fieldName).append(": ").append(bounds.get(fieldName));
      first = false;
    }
    sb.append("}");
    return sb.toString();
  }

  private String bytesToHex(byte[] bytes, int length) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      sb.append(String.format("%02x ", bytes[i]));
    }
    return sb.toString().trim();
  }
}
