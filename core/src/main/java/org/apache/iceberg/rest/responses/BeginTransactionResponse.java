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
package org.apache.iceberg.rest.responses;

import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTResponse;

/** Represents a REST response for a request to create a namespace / database. */
public class BeginTransactionResponse implements RESTResponse {

  private String transactionId;

  public BeginTransactionResponse() {
    // Required for Jackson deserialization.
  }

  private BeginTransactionResponse(String transactionId) {
    this.transactionId = transactionId;
    validate();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(transactionId != null, "Invalid transaction-id: null");
  }

  public String transactionId() {
    return transactionId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("transaction-id", transactionId).toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String transactionId;

    private Builder() {}

    public Builder withTransactionId(String transactionId) {
      Preconditions.checkNotNull(transactionId, "Invalid transaction-id: null");
      this.transactionId = transactionId;
      return this;
    }

    public BeginTransactionResponse build() {
      return new BeginTransactionResponse(transactionId);
    }
  }
}
