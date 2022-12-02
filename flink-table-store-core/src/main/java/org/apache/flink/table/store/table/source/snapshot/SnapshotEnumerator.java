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

package org.apache.flink.table.store.table.source.snapshot;

import org.apache.flink.table.store.table.source.DataTableScan;

import javax.annotation.Nullable;

/** Enumerate incremental changes from newly created snapshots. */
public interface SnapshotEnumerator {

    /**
     * The first call to this method will produce a {@link DataTableScan.DataFilePlan} containing
     * the base files for the following incremental changes (or just return null if there are no
     * base files).
     *
     * <p>Following calls to this method will produce {@link DataTableScan.DataFilePlan}s containing
     * incremental changed files. If there is currently no newer snapshots, null will be returned
     * instead.
     */
    @Nullable
    DataTableScan.DataFilePlan enumerate();
}
