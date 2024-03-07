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

package org.apache.paimon.table;

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.RowType;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A table provides basic abstraction for table type and table scan and table read.
 *
 * @since 0.4.0
 */
@Public
public interface Table extends Serializable {

    // ================== Table Metadata =====================

    /** A name to identify this table. */
    String name();

    /** Returns the row type of this table. */
    RowType rowType();

    /** Partition keys of this table. */
    List<String> partitionKeys();

    /** Primary keys of this table. */
    List<String> primaryKeys();

    /** Options of this table. */
    Map<String, String> options();

    /** Optional comment of this table. */
    Optional<String> comment();

    /** Optional statistics of this table. */
    @Experimental
    Optional<Statistics> statistics();

    // ================= Table Operations ====================

    /** Copy this table with adding dynamic options. */
    Table copy(Map<String, String> dynamicOptions);

    /** Rollback table's state to a specific snapshot. */
    @Experimental
    void rollbackTo(long snapshotId);

    /** Create a tag from given snapshot. */
    @Experimental
    void createTag(String tagName, long fromSnapshotId);

    @Experimental
    void createTag(String tagName, long fromSnapshotId, Duration timeRetained);

    /** Create a tag from latest snapshot. */
    @Experimental
    void createTag(String tagName);

    @Experimental
    void createTag(String tagName, Duration timeRetained);

    /** Delete a tag by name. */
    @Experimental
    void deleteTag(String tagName);

    /** Rollback table's state to a specific tag. */
    @Experimental
    void rollbackTo(String tagName);

    /** Create a empty branch. */
    @Experimental
    void createBranch(String branchName);

    /** Create a branch from given snapshot. */
    @Experimental
    void createBranch(String branchName, long snapshotId);

    /** Create a branch from given tag. */
    @Experimental
    void createBranch(String branchName, String tagName);

    /** Delete a branch by branchName. */
    @Experimental
    void deleteBranch(String branchName);

    @Experimental
    void replaceBranch(String fromBranch);

    /** Replace main branch. */
    @Experimental
    void replaceMainBranch(String branchName);

    @Experimental
    void cleanMainBranchFile();

    @Experimental
    void mainBranch();

    /** Manually expire snapshots, parameters can be controlled independently of table options. */
    @Experimental
    ExpireSnapshots newExpireSnapshots();

    @Experimental
    ExpireSnapshots newExpireChangelog();

    // =============== Read & Write Operations ==================

    /** Returns a new read builder. */
    ReadBuilder newReadBuilder();

    /** Returns a new batch write builder. */
    BatchWriteBuilder newBatchWriteBuilder();

    /** Returns a new stream write builder. */
    StreamWriteBuilder newStreamWriteBuilder();
}
