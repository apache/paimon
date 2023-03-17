/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.table.sink;

import org.apache.flink.table.store.table.Table;

import org.apache.paimon.annotation.Experimental;

/**
 * Create and commit snapshots of a {@link Table}. Snapshots are produced from {@link
 * CommitMessage}s, which themselves are generated by {@link TableWrite}.
 *
 * <p>Commit may fail and throw an exception. Before committing, it will first check for conflicts
 * by checking if all files to be removed currently exists.
 *
 * @since 0.4.0
 */
@Experimental
public interface TableCommit extends AutoCloseable {}
