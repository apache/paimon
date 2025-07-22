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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.FileIndexEvaluator;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStatsEvolution;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** {@link FileStoreScan} for {@link AppendOnlyFileStore}. */
public class AppendOnlyFileStoreScan extends AbstractFileStoreScan {

    private final BucketSelectConverter bucketSelectConverter;
    private final SimpleStatsEvolutions simpleStatsEvolutions;

    private final boolean fileIndexReadEnabled;

    private Predicate inputFilter;

    // cache not evolved filter by schema id
    private final Map<Long, Predicate> notEvolvedFilterMapping = new ConcurrentHashMap<>();

    // cache evolved filter by schema id
    private final Map<Long, Predicate> evolvedFilterMapping = new ConcurrentHashMap<>();

    private FileIO fileIO;

    private FileStorePathFactory pathFactory;

    public AppendOnlyFileStoreScan(
            ManifestsReader manifestsReader,
            BucketSelectConverter bucketSelectConverter,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            ManifestFile.Factory manifestFileFactory,
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            Integer scanManifestParallelism,
            boolean fileIndexReadEnabled) {
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
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
    }

    public AppendOnlyFileStoreScan withFilter(Predicate predicate) {
        this.inputFilter = predicate;
        this.bucketSelectConverter.convert(predicate).ifPresent(this::withTotalAwareBucketFilter);
        return this;
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
        TableSchema tableSchema = scanTableSchema(entry.file().schemaId());
        Predicate dataPredicate =
                evolvedFilterMapping.computeIfAbsent(
                        entry.file().schemaId(),
                        id ->
                                simpleStatsEvolutions.tryDevolveFilter(
                                        entry.file().schemaId(), inputFilter));

        DataFilePathFactory dataFilePathFactory =
                pathFactory.createDataFilePathFactory(entry.partition(), entry.bucket());
        try {
            return FileIndexEvaluator.evaluate(
                            fileIO, tableSchema, dataPredicate, dataFilePathFactory, entry.file())
                    .remain();
        } catch (IOException e) {
            throw new RuntimeException("Exception happens while checking file index.", e);
        }
    }
}
