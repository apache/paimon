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

import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.LocalOrphanFilesClean;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.DVMetaCache;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.TagManager;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * An abstraction layer above {@link FileStore} to provide reading and writing of {@link
 * InternalRow}.
 */
public interface FileStoreTable extends DataTable {

    void setManifestCache(SegmentsCache<Path> manifestCache);

    @Nullable
    SegmentsCache<Path> getManifestCache();

    void setSnapshotCache(Cache<Path, Snapshot> cache);

    void setStatsCache(Cache<String, Statistics> cache);

    void setDVMetaCache(DVMetaCache cache);

    @Override
    default RowType rowType() {
        return schema().logicalRowType();
    }

    @Override
    default List<String> partitionKeys() {
        return schema().partitionKeys();
    }

    @Override
    default List<String> primaryKeys() {
        return schema().primaryKeys();
    }

    default BucketSpec bucketSpec() {
        return new BucketSpec(bucketMode(), schema().bucketKeys(), schema().numBuckets());
    }

    default BucketMode bucketMode() {
        return store().bucketMode();
    }

    @Override
    default Map<String, String> options() {
        return schema().options();
    }

    @Override
    default Optional<String> comment() {
        return Optional.ofNullable(schema().comment());
    }

    TableSchema schema();

    FileStore<?> store();

    CatalogEnvironment catalogEnvironment();

    @Override
    FileStoreTable copy(Map<String, String> dynamicOptions);

    FileStoreTable copy(TableSchema newTableSchema);

    /** Doesn't change table schema even when there exists time travel scan options. */
    FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions);

    /** TODO: this method is weird, old options will overwrite new options. */
    FileStoreTable copyWithLatestSchema();

    @Override
    TableWriteImpl<?> newWrite(String commitUser);

    TableWriteImpl<?> newWrite(String commitUser, @Nullable Integer writeId);

    @Override
    TableCommitImpl newCommit(String commitUser);

    LocalTableQuery newLocalTableQuery();

    boolean supportStreamingReadOverwrite();

    RowKeyExtractor createRowKeyExtractor();

    /**
     * Get {@link DataTable} with branch identified by {@code branchName}. Note that this method
     * does not keep dynamic options in current table.
     */
    @Override
    FileStoreTable switchToBranch(String branchName);

    TagAutoManager newTagAutoManager();

    /** Purge all files in this table. */
    default void purgeFiles() throws Exception {
        // clear branches
        BranchManager branchManager = branchManager();
        branchManager.branches().forEach(branchManager::dropBranch);

        // clear tags
        TagManager tagManager = tagManager();
        tagManager.allTagNames().forEach(this::deleteTag);

        // clear consumers
        ConsumerManager consumerManager = this.consumerManager();
        consumerManager.consumers().keySet().forEach(consumerManager::deleteConsumer);

        // truncate table
        try (BatchTableCommit commit = this.newBatchWriteBuilder().newCommit()) {
            commit.truncateTable();
        }

        // clear changelogs
        ChangelogManager changelogManager = this.changelogManager();
        this.fileIO().delete(changelogManager.changelogDirectory(), true);

        // clear snapshots, keep only latest snapshot
        this.newExpireSnapshots()
                .config(
                        ExpireConfig.builder()
                                .snapshotMaxDeletes(Integer.MAX_VALUE)
                                .snapshotRetainMax(1)
                                .snapshotRetainMin(1)
                                .snapshotTimeRetain(Duration.ZERO)
                                .build())
                .expire();

        // clear orphan files
        LocalOrphanFilesClean clean = new LocalOrphanFilesClean(this, System.currentTimeMillis());
        clean.clean();
    }
}
