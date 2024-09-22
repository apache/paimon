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

package org.apache.paimon.operation;

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.fileindex.FileIndexPredicate;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.stats.SimpleStatsConverters;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** {@link FileStoreScan} for {@link AppendOnlyFileStore}. */
public class AppendOnlyFileStoreScan extends AbstractFileStoreScan {

    private final SimpleStatsConverters simpleStatsConverters;

    private final boolean fileIndexReadEnabled;

    private Predicate filter;

    // just cache.
    private final Map<Long, Predicate> dataFilterMapping = new HashMap<>();

    public AppendOnlyFileStoreScan(
            ManifestsReader manifestsReader,
            RowType partitionType,
            ScanBucketFilter bucketFilter,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            ManifestFile.Factory manifestFileFactory,
            int numOfBuckets,
            boolean checkNumOfBuckets,
            Integer scanManifestParallelism,
            boolean fileIndexReadEnabled) {
        super(
                manifestsReader,
                partitionType,
                bucketFilter,
                snapshotManager,
                schemaManager,
                schema,
                manifestFileFactory,
                numOfBuckets,
                checkNumOfBuckets,
                scanManifestParallelism);
        this.simpleStatsConverters =
                new SimpleStatsConverters(sid -> scanTableSchema(sid).fields(), schema.id());
        this.fileIndexReadEnabled = fileIndexReadEnabled;
    }

    public AppendOnlyFileStoreScan withFilter(Predicate predicate) {
        this.filter = predicate;
        this.bucketKeyFilter.pushdown(predicate);
        return this;
    }

    /** Note: Keep this thread-safe. */
    @Override
    protected boolean filterByStats(ManifestEntry entry) {
        if (filter == null) {
            return true;
        }

        SimpleStatsConverter serializer =
                simpleStatsConverters.getOrCreate(entry.file().schemaId());
        SimpleStats stats = entry.file().valueStats();

        return filter.test(
                        entry.file().rowCount(),
                        serializer.evolution(stats.minValues()),
                        serializer.evolution(stats.maxValues()),
                        serializer.evolution(stats.nullCounts(), entry.file().rowCount()))
                && (!fileIndexReadEnabled || testFileIndex(entry.file().embeddedIndex(), entry));
    }

    @Override
    protected List<ManifestEntry> filterWholeBucketByStats(List<ManifestEntry> entries) {
        // We don't need to filter per-bucket entries here
        return entries;
    }

    private boolean testFileIndex(@Nullable byte[] embeddedIndexBytes, ManifestEntry entry) {
        if (embeddedIndexBytes == null) {
            return true;
        }

        RowType dataRowType = scanTableSchema(entry.file().schemaId()).logicalRowType();

        Predicate dataPredicate =
                dataFilterMapping.computeIfAbsent(
                        entry.file().schemaId(),
                        id -> simpleStatsConverters.convertFilter(entry.file().schemaId(), filter));

        try (FileIndexPredicate predicate =
                new FileIndexPredicate(embeddedIndexBytes, dataRowType)) {
            return predicate.testPredicate(dataPredicate);
        } catch (IOException e) {
            throw new RuntimeException("Exception happens while checking predicate.", e);
        }
    }
}
