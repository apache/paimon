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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFile;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.stats.FieldStatsConverters;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.types.RowType;

import java.util.List;

import static org.apache.flink.table.store.file.predicate.PredicateBuilder.and;
import static org.apache.flink.table.store.file.predicate.PredicateBuilder.pickTransformFieldMapping;
import static org.apache.flink.table.store.file.predicate.PredicateBuilder.splitAnd;

/** {@link FileStoreScan} for {@link org.apache.flink.table.store.file.AppendOnlyFileStore}. */
public class AppendOnlyFileStoreScan extends AbstractFileStoreScan {

    private final RowType rowType;
    private final FieldStatsConverters fieldStatsConverters;

    private Predicate filter;

    public AppendOnlyFileStoreScan(
            RowType partitionType,
            RowType bucketKeyType,
            RowType rowType,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            long schemaId,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory,
            int numOfBuckets,
            boolean checkNumOfBuckets) {
        super(
                partitionType,
                bucketKeyType,
                snapshotManager,
                schemaManager,
                schemaId,
                manifestFileFactory,
                manifestListFactory,
                numOfBuckets,
                checkNumOfBuckets);
        this.rowType = rowType;
        this.fieldStatsConverters =
                new FieldStatsConverters(sid -> scanTableSchema(sid).fields(), schemaId);
    }

    public AppendOnlyFileStoreScan withFilter(Predicate predicate) {
        this.filter = predicate;

        List<Predicate> bucketFilters =
                pickTransformFieldMapping(
                        splitAnd(predicate),
                        rowType.getFieldNames(),
                        bucketKeyType.getFieldNames());
        if (bucketFilters.size() > 0) {
            withBucketKeyFilter(and(bucketFilters));
        }
        return this;
    }

    /** Note: Keep this thread-safe. */
    @Override
    protected boolean filterByStats(ManifestEntry entry) {
        return filter == null
                || filter.test(
                        entry.file().rowCount(),
                        entry.file()
                                .valueStats()
                                .fields(
                                        fieldStatsConverters.getOrCreate(entry.file().schemaId()),
                                        entry.file().rowCount()));
    }
}
