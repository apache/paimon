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

import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.FieldStatsConverters;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import java.util.List;

import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.pickTransformFieldMapping;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;

/** {@link FileStoreScan} for {@link KeyValueFileStore}. */
public class KeyValueFileStoreScan extends AbstractFileStoreScan {

    private final FieldStatsConverters fieldStatsConverters;
    private final RowType keyType;

    private Predicate keyFilter;

    public KeyValueFileStoreScan(
            RowType partitionType,
            RowType bucketKeyType,
            RowType keyType,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            long schemaId,
            KeyValueFieldsExtractor keyValueFieldsExtractor,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory,
            int numOfBuckets,
            boolean checkNumOfBuckets) {
        super(
                partitionType,
                bucketKeyType,
                snapshotManager,
                schemaManager,
                manifestFileFactory,
                manifestListFactory,
                numOfBuckets,
                checkNumOfBuckets);
        this.fieldStatsConverters =
                new FieldStatsConverters(
                        sid -> keyValueFieldsExtractor.keyFields(scanTableSchema(sid)), schemaId);
        this.keyType = keyType;
    }

    public KeyValueFileStoreScan withKeyFilter(Predicate predicate) {
        this.keyFilter = predicate;

        List<Predicate> bucketFilters =
                pickTransformFieldMapping(
                        splitAnd(predicate),
                        keyType.getFieldNames(),
                        bucketKeyType.getFieldNames());
        if (bucketFilters.size() > 0) {
            withBucketKeyFilter(and(bucketFilters));
        }
        return this;
    }

    /** Note: Keep this thread-safe. */
    @Override
    protected boolean filterByStats(ManifestEntry entry) {
        return keyFilter == null
                || keyFilter.test(
                        entry.file().rowCount(),
                        entry.file()
                                .keyStats()
                                .fields(
                                        fieldStatsConverters.getOrCreate(entry.file().schemaId()),
                                        entry.file().rowCount()));
    }
}
