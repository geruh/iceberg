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
package org.apache.iceberg.rest.requests;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.RESTRequest;

public class UpdateTableRequest implements RESTRequest {

  private TableIdentifier identifier;
  private List<org.apache.iceberg.UpdateRequirement> requirements;
  private List<MetadataUpdate> updates;

  // Transient field used only for serialization - not included in JSON
  private transient Map<Integer, PartitionSpec> specsById;

  public UpdateTableRequest() {
    // needed for Jackson deserialization
  }

  public UpdateTableRequest(
      List<org.apache.iceberg.UpdateRequirement> requirements, List<MetadataUpdate> updates) {
    this.requirements = requirements;
    this.updates = updates;
  }

  public UpdateTableRequest(
      List<org.apache.iceberg.UpdateRequirement> requirements,
      List<MetadataUpdate> updates,
      Map<Integer, PartitionSpec> specsById) {
    this.requirements = requirements;
    this.updates = updates;
    this.specsById = specsById;
  }

  UpdateTableRequest(
      TableIdentifier identifier,
      List<org.apache.iceberg.UpdateRequirement> requirements,
      List<MetadataUpdate> updates) {
    this(requirements, updates);
    this.identifier = identifier;
  }

  @Override
  public void validate() {}

  public List<org.apache.iceberg.UpdateRequirement> requirements() {
    return requirements != null ? requirements : ImmutableList.of();
  }

  public List<MetadataUpdate> updates() {
    return updates != null ? updates : ImmutableList.of();
  }

  public TableIdentifier identifier() {
    return identifier;
  }

  /**
   * Returns partition specs by ID, used for serializing ProduceSnapshotUpdate.
   *
   * <p>This is a transient field used only during client-side serialization. It is not included in
   * the JSON representation of the request.
   *
   * @return partition specs by ID, or null if not set
   */
  public Map<Integer, PartitionSpec> specsById() {
    return specsById;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("requirements", requirements)
        .add("updates", updates)
        .toString();
  }

  public static UpdateTableRequest create(
      TableIdentifier identifier,
      List<org.apache.iceberg.UpdateRequirement> requirements,
      List<MetadataUpdate> updates) {
    return new UpdateTableRequest(identifier, requirements, updates);
  }
}
