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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fileindex.FileIndexPredicate;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.BinaryTableStats;
import org.apache.paimon.stats.FieldStatsArraySerializer;
import org.apache.paimon.stats.FieldStatsConverters;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** {@link FileStoreScan} for {@link AppendOnlyFileStore}. */
public class AppendOnlyFileStoreScan extends AbstractFileStoreScan {

    private final FieldStatsConverters fieldStatsConverters;

    private Predicate filter;

    private final Map<Long, Predicate> dataFilterMapping = new HashMap<>();

    public AppendOnlyFileStoreScan(
            RowType partitionType,
            ScanBucketFilter bucketFilter,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory,
            int numOfBuckets,
            boolean checkNumOfBuckets,
            Integer scanManifestParallelism,
            String branchName) {
        super(
                partitionType,
                bucketFilter,
                snapshotManager,
                schemaManager,
                schema,
                manifestFileFactory,
                manifestListFactory,
                numOfBuckets,
                checkNumOfBuckets,
                scanManifestParallelism,
                branchName);
        this.fieldStatsConverters =
                new FieldStatsConverters(sid -> scanTableSchema(sid).fields(), schema.id());
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

        FieldStatsArraySerializer serializer =
                fieldStatsConverters.getOrCreate(entry.file().schemaId());
        BinaryTableStats stats = entry.file().valueStats();

        return filter.test(
                        entry.file().rowCount(),
                        serializer.evolution(stats.minValues()),
                        serializer.evolution(stats.maxValues()),
                        serializer.evolution(stats.nullCounts(), entry.file().rowCount()))
                && testFileIndex(entry.file().filter(), entry);
    }

    @Override
    protected List<ManifestEntry> filterWholeBucketByStats(List<ManifestEntry> entries) {
        // We don't need to filter per-bucket entries here
        return entries;
    }

    private boolean testFileIndex(BinaryRow filterRow, ManifestEntry entry) {
        if (filterRow.getFieldCount() == 0 || filterRow.isNullAt(0)) {
            return true;
        }

        RowType dataRowType = scanTableSchema(entry.file().schemaId()).logicalRowType();

        Predicate dataPredicate =
                dataFilterMapping.computeIfAbsent(
                        entry.file().schemaId(),
                        id -> fieldStatsConverters.convertFilter(entry.file().schemaId(), filter));

        try (FileIndexPredicate predicate =
                new FileIndexPredicate(filterRow.getBinary(0), dataRowType)) {
            return predicate.testPredicate(dataPredicate);
        } catch (IOException e) {
            throw new RuntimeException("Exception happens while checking predicate.", e);
        }
    }
}
