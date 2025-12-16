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
package org.apache.iceberg.expressions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * This is Iceberg's representation of variant access information, similar to Spark's
 * VariantAccessInfo but with additional helper
 */
public final class VariantAccessInfo implements Serializable, Comparable<VariantAccessInfo> {
  private final String columnName;
  private final List<String> path;
  private transient int hashCode;
  private transient boolean hashCodeComputed;

  private VariantAccessInfo(String columnName, List<String> path) {
    Preconditions.checkNotNull(columnName, "Column name cannot be null");
    Preconditions.checkNotNull(path, "Path cannot be null");
    this.columnName = columnName;
    this.path = ImmutableList.copyOf(path);
  }

  /**
   * Creates a VariantAccessInfo by parsing a JSON path string.
   *
   * @param columnName the name of the variant column being accessed
   * @param jsonPath a JSON path like "$.name" or "$.address.city"
   * @return the VariantAccessInfo
   * @throws IllegalArgumentException if the path is invalid
   */
  public static VariantAccessInfo parse(String columnName, String jsonPath) {
    List<String> parsed = PathUtil.parse(jsonPath);
    return new VariantAccessInfo(columnName, parsed);
  }

  /**
   * Creates a VariantAccessInfo from path elements.
   *
   * @param columnName the name of the variant column being accessed
   * @param pathElements the field names (without the root "$")
   * @return the VariantAccessInfo
   */
  public static VariantAccessInfo of(String columnName, String... pathElements) {
    return new VariantAccessInfo(columnName, Arrays.asList(pathElements));
  }

  /**
   * Creates a VariantAccessInfo from a list of path elements.
   *
   * @param columnName the name of the variant column being accessed
   * @param pathElements the field names (without the root "$")
   * @return the VariantAccessInfo
   */
  public static VariantAccessInfo of(String columnName, List<String> pathElements) {
    return new VariantAccessInfo(columnName, pathElements);
  }

  /** Returns the name of the variant column being accessed. */
  public String columnName() {
    return columnName;
  }

  /** Returns the path elements (without the root "$"). */
  public List<String> path() {
    return path;
  }

  /** Returns the depth (number of elements) of the path. */
  public int depth() {
    return path.size();
  }

  /** Returns true if this accesses the root of the variant ($). */
  public boolean isRoot() {
    return path.isEmpty();
  }

  /**
   * Returns the path element at the given index.
   *
   * @param index the index (0-based)
   * @return the field name at that index
   * @throws IndexOutOfBoundsException if index is out of range
   */
  public String get(int index) {
    return path.get(index);
  }

  /**
   * Returns the JSON path string representation.
   *
   * @return the normalized JSON path (e.g., "$['name']" or "$['address']['city']")
   */
  public String toJsonPath() {
    return PathUtil.toNormalizedPath(path);
  }

  /**
   * Returns true if this access's path is a prefix of the given path elements.
   *
   */
  public boolean pathIsPrefixOf(List<String> otherPath) {
    if (this.path.size() > otherPath.size()) {
      return false;
    }

    for (int i = 0; i < this.path.size(); i++) {
      if (!this.path.get(i).equals(otherPath.get(i))) {
        return false;
      }
    }

    return true;
  }

  /**
   * Returns true if this access's path matches for column pruning purposes.
   */
  public boolean matchesForPruning(List<String> fieldPath) {
    // Check if this.path is a prefix of fieldPath
    if (pathIsPrefixOf(fieldPath)) {
      return true;
    }

    // Check if fieldPath is a prefix of this.path
    if (fieldPath.size() > this.path.size()) {
      return false;
    }

    for (int i = 0; i < fieldPath.size(); i++) {
      if (!fieldPath.get(i).equals(this.path.get(i))) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int compareTo(VariantAccessInfo other) {
    int cmp = this.columnName.compareTo(other.columnName);
    if (cmp != 0) {
      return cmp;
    }

    int minDepth = Math.min(this.depth(), other.depth());
    for (int i = 0; i < minDepth; i++) {
      cmp = this.path.get(i).compareTo(other.path.get(i));
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(this.depth(), other.depth());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof VariantAccessInfo)) {
      return false;
    }
    VariantAccessInfo other = (VariantAccessInfo) obj;
    return columnName.equals(other.columnName) && path.equals(other.path);
  }

  @Override
  public int hashCode() {
    if (!hashCodeComputed) {
      hashCode = Objects.hash(columnName, path);
      hashCodeComputed = true;
    }
    return hashCode;
  }

  @Override
  public String toString() {
    if (path.isEmpty()) {
      return columnName + ":$";
    }
    return columnName + ":$." + String.join(".", path);
  }
}
