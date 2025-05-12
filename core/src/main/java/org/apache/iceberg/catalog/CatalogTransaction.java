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
package org.apache.iceberg.catalog;

import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.exceptions.CommitFailedException;

public interface CatalogTransaction {

  /** The current {@link IsolationLevel} for this transaction. */
  IsolationLevel isolationLevel();

  /**
   * Return a point-in-time snapshot {@link Catalog} leveraging the sequence number participating in
   * the transaction.
   */
  Catalog asCatalog();

  /**
   * Commit all changes made in this transaction based on the following: Collecting all TableUpdates
   * made across tables, Performing appropriate validations based on isolation level, Executing
   * rebasing logic when permitted by the isolation level and Ensuring atomic commit of all changes
   *
   * @throws CommitFailedException If the transaction failed
   */
  void commitTransaction();
}
