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
import org.apache.paimon.stats.SimpleStatsEvolution;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** {@link FileStoreScan} for {@link AppendOnlyFileStore}. */
public class AppendOnlyFileStoreScan extends AbstractFileStoreScan {

    private final BucketSelectConverter bucketSelectConverter;
    private final SimpleStatsEvolutions simpleStatsEvolutions;

    private final boolean fileIndexReadEnabled;
    private final boolean deletionVectorsEnabled;

    protected Predicate inputFilter;

    // cache not evolved filter by schema id
    private final Map<Long, Predicate> notEvolvedFilterMapping = new ConcurrentHashMap<>();

    // cache evolved filter by schema id
    private final Map<Long, Predicate> evolvedFilterMapping = new ConcurrentHashMap<>();

    public AppendOnlyFileStoreScan(
            ManifestsReader manifestsReader,
            BucketSelectConverter bucketSelectConverter,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            ManifestFile.Factory manifestFileFactory,
            Integer scanManifestParallelism,
            boolean fileIndexReadEnabled,
            boolean deletionVectorsEnabled) {
        super(
                manifestsReader,
                snapshotManager,
                schemaManager,
                schema,
                manifestFileFactory,
                scanManifestParallelism);
        this.bucketSelectConverter = bucketSelectConverter;
        this.simpleStatsEvolutions =
                new SimpleStatsEvolutions(sid -> scanTableSchema(sid).fields(), schema.id());
        this.fileIndexReadEnabled = fileIndexReadEnabled;
        this.deletionVectorsEnabled = deletionVectorsEnabled;
    }

    public AppendOnlyFileStoreScan withFilter(Predicate predicate) {
        this.inputFilter = predicate;
        this.bucketSelectConverter.convert(predicate).ifPresent(this::withTotalAwareBucketFilter);
        return this;
    }

    @Override
    public boolean supportsLimitPushManifestEntries() {
        return limit != null && limit > 0 && !deletionVectorsEnabled;
    }

    @Override
    protected Iterator<ManifestEntry> limitPushManifestEntries(Iterator<ManifestEntry> entries) {
        checkArgument(limit != null && limit > 0 && !deletionVectorsEnabled);
        return new LimitAwareManifestEntryIterator(entries, limit);
    }

    /** Note: Keep this thread-safe. */
    @Override
    protected boolean filterByStats(ManifestEntry entry) {
        Predicate notEvolvedFilter =
                notEvolvedFilterMapping.computeIfAbsent(
                        entry.file().schemaId(),
                        id ->
                                // keepNewFieldFilter to handle add field
                                // for example, add field 'c', 'c > 3': old files can be filtered
                                simpleStatsEvolutions.filterUnsafeFilter(
                                        entry.file().schemaId(), inputFilter, true));
        if (notEvolvedFilter == null) {
            return true;
        }

        SimpleStatsEvolution evolution = simpleStatsEvolutions.getOrCreate(entry.file().schemaId());
        SimpleStatsEvolution.Result stats =
                evolution.evolution(
                        entry.file().valueStats(),
                        entry.file().rowCount(),
                        entry.file().valueStatsCols());

        // filter by min max
        boolean result =
                notEvolvedFilter.test(
                        entry.file().rowCount(),
                        stats.minValues(),
                        stats.maxValues(),
                        stats.nullCounts());

        if (!result) {
            return false;
        }

        if (!fileIndexReadEnabled) {
            return true;
        }

        return testFileIndex(entry.file().embeddedIndex(), entry);
    }

    private boolean testFileIndex(@Nullable byte[] embeddedIndexBytes, ManifestEntry entry) {
        if (embeddedIndexBytes == null) {
            return true;
        }

        RowType dataRowType = scanTableSchema(entry.file().schemaId()).logicalRowType();

        Predicate dataPredicate =
                evolvedFilterMapping.computeIfAbsent(
                        entry.file().schemaId(),
                        id ->
                                simpleStatsEvolutions.tryDevolveFilter(
                                        entry.file().schemaId(), inputFilter));

        try (FileIndexPredicate predicate =
                new FileIndexPredicate(embeddedIndexBytes, dataRowType)) {
            return predicate.evaluate(dataPredicate).remain();
        } catch (IOException e) {
            throw new RuntimeException("Exception happens while checking predicate.", e);
        }
    }

    /**
     * Iterator that applies limit pushdown by stopping early when enough rows have been
     * accumulated.
     */
    private static class LimitAwareManifestEntryIterator implements Iterator<ManifestEntry> {
        private final Iterator<ManifestEntry> baseIterator;
        private final long limit;

        private long accumulatedRowCount = 0;
        private ManifestEntry nextEntry = null;
        private boolean hasNext = false;

        LimitAwareManifestEntryIterator(Iterator<ManifestEntry> baseIterator, long limit) {
            this.baseIterator = baseIterator;
            this.limit = limit;
            advance();
        }

        private void advance() {
            // If we've already accumulated enough rows, stop reading more entries
            if (accumulatedRowCount >= limit) {
                hasNext = false;
                nextEntry = null;
                return;
            }

            if (baseIterator.hasNext()) {
                nextEntry = baseIterator.next();
                hasNext = true;

                long fileRowCount = nextEntry.file().rowCount();
                if (fileRowCount > 0) {
                    accumulatedRowCount += fileRowCount;
                }

                return;
            }

            // No more base entries
            hasNext = false;
            nextEntry = null;
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public ManifestEntry next() {
            // This exception is only thrown if next() is called when hasNext() returns false.
            if (!hasNext) {
                throw new java.util.NoSuchElementException();
            }
            ManifestEntry current = nextEntry;
            advance();
            return current;
        }
    }
}
