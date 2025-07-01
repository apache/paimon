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
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStatsEvolution;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.utils.SnapshotManager;

/** {@link FileStoreScan} for {@link AppendOnlyFileStore}. */
public class AppendOnlyFileStoreScan extends AbstractFileStoreScan {

    private final BucketSelectConverter bucketSelectConverter;
    private final SimpleStatsEvolutions simpleStatsEvolutions;

    private Predicate filter;

    public AppendOnlyFileStoreScan(
            ManifestsReader manifestsReader,
            BucketSelectConverter bucketSelectConverter,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            ManifestFile.Factory manifestFileFactory,
            Integer scanManifestParallelism,
            boolean fileIndexReadEnabled) {
        super(
                manifestsReader,
                snapshotManager,
                schemaManager,
                schema,
                manifestFileFactory,
                scanManifestParallelism,
                fileIndexReadEnabled);
        this.bucketSelectConverter = bucketSelectConverter;
        this.simpleStatsEvolutions =
                new SimpleStatsEvolutions(sid -> scanTableSchema(sid).fields(), schema.id());
    }

    public AppendOnlyFileStoreScan withFilter(Predicate predicate) {
        this.filter = predicate;
        this.bucketSelectConverter.convert(predicate).ifPresent(this::withTotalAwareBucketFilter);
        return this;
    }

    /** Note: Keep this thread-safe. */
    @Override
    protected boolean filterByStats(ManifestEntry entry) {
        if (filter == null) {
            return true;
        }

        SimpleStatsEvolution evolution = simpleStatsEvolutions.getOrCreate(entry.file().schemaId());
        SimpleStatsEvolution.Result stats =
                evolution.evolution(
                        entry.file().valueStats(),
                        entry.file().rowCount(),
                        entry.file().valueStatsCols());

        return filter.test(
                entry.file().rowCount(), stats.minValues(), stats.maxValues(), stats.nullCounts());
    }

    @Override
    protected Predicate convertFilter(ManifestEntry entry) {
        return filter == null
                ? null
                : simpleStatsEvolutions.tryDevolveFilter(entry.file().schemaId(), filter);
    }
}
