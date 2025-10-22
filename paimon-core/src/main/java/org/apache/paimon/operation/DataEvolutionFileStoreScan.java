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

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.DataEvolutionArray;
import org.apache.paimon.reader.DataEvolutionRow;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsEvolution;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.source.DataEvolutionSplitGenerator;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** {@link FileStoreScan} for data-evolution enabled table. */
public class DataEvolutionFileStoreScan extends AppendOnlyFileStoreScan {

    public DataEvolutionFileStoreScan(
            ManifestsReader manifestsReader,
            BucketSelectConverter bucketSelectConverter,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            ManifestFile.Factory manifestFileFactory,
            Integer scanManifestParallelism) {
        super(
                manifestsReader,
                bucketSelectConverter,
                snapshotManager,
                schemaManager,
                schema,
                manifestFileFactory,
                scanManifestParallelism,
                false);
    }

    public DataEvolutionFileStoreScan withFilter(Predicate predicate) {
        this.inputFilter = predicate;
        return this;
    }

    @Override
    protected List<ManifestEntry> postFilter(List<ManifestEntry> entries) {
        List<List<FakeDataFileMeta>> splitByRowId =
                DataEvolutionSplitGenerator.split(
                        entries.stream().map(FakeDataFileMeta::new).collect(Collectors.toList()));

        return splitByRowId.stream()
                .filter(this::filterByStats)
                .flatMap(s -> s.stream().map(r -> r.entry))
                .collect(Collectors.toList());
    }

    private boolean filterByStats(List<FakeDataFileMeta> metas) {
        long rowCount = metas.get(0).rowCount();
        SimpleStatsEvolution.Result evolutionResult = evolutionStats(metas);
        return inputFilter.test(
                rowCount,
                evolutionResult.minValues(),
                evolutionResult.maxValues(),
                evolutionResult.nullCounts());
    }

    private SimpleStatsEvolution.Result evolutionStats(List<FakeDataFileMeta> metas) {
        int[] allFields = schema.fields().stream().mapToInt(DataField::id).toArray();
        int fieldsCount = schema.fields().size();
        int[] rowOffsets = new int[fieldsCount];
        int[] fieldOffsets = new int[fieldsCount];
        Arrays.fill(rowOffsets, -1);
        Arrays.fill(fieldOffsets, -1);

        InternalRow[] min = new InternalRow[metas.size()];
        InternalRow[] max = new InternalRow[metas.size()];
        BinaryArray[] nullCounts = new BinaryArray[metas.size()];

        for (int i = 0; i < metas.size(); i++) {
            SimpleStats stats = metas.get(i).valueStats();
            min[i] = stats.minValues();
            max[i] = stats.maxValues();
            nullCounts[i] = stats.nullCounts();
        }

        for (int i = 0; i < metas.size(); i++) {
            FakeDataFileMeta fileMeta = metas.get(i);
            TableSchema dataFileSchema =
                    scanTableSchema(fileMeta.schemaId())
                            .project(
                                    fileMeta.valueStatsCols() == null
                                            ? fileMeta.writeCols()
                                            : fileMeta.valueStatsCols());
            int[] fieldIds =
                    SpecialFields.rowTypeWithRowTracking(dataFileSchema.logicalRowType())
                            .getFields().stream()
                            .mapToInt(DataField::id)
                            .toArray();

            int count = 0;
            for (int j = 0; j < fieldsCount; j++) {
                for (int fieldId : fieldIds) {
                    if (allFields[j] == fieldId) {
                        // TODO: If type not match (e.g. int -> string), we need to skip this, set
                        // rowOffsets[j] = -1 always. (may -2, after all, set it back to -1)
                        // Because schema evolution may happen to change int to string or something
                        // like that.
                        if (rowOffsets[j] == -1) {
                            rowOffsets[j] = i;
                            fieldOffsets[j] = count++;
                        }
                        break;
                    }
                }
            }
        }

        DataEvolutionRow finalMin = new DataEvolutionRow(metas.size(), rowOffsets, fieldOffsets);
        DataEvolutionRow finalMax = new DataEvolutionRow(metas.size(), rowOffsets, fieldOffsets);
        DataEvolutionArray finalNullCounts =
                new DataEvolutionArray(metas.size(), rowOffsets, fieldOffsets);

        finalMin.setRows(min);
        finalMax.setRows(max);
        finalNullCounts.setRows(nullCounts);
        return new SimpleStatsEvolution.Result(finalMin, finalMax, finalNullCounts);
    }

    /** Note: Keep this thread-safe. */
    @Override
    protected boolean filterByStats(ManifestEntry entry) {
        return true;
    }

    private static class FakeDataFileMeta implements DataFileMeta {
        private final ManifestEntry entry;

        FakeDataFileMeta(ManifestEntry entry) {
            this.entry = entry;
        }

        public ManifestEntry entry() {
            return entry;
        }

        @Override
        public String fileName() {
            return entry.file().fileName();
        }

        @Override
        public long fileSize() {
            return entry.file().fileSize();
        }

        @Override
        public long rowCount() {
            return entry.file().rowCount();
        }

        @Override
        public Optional<Long> deleteRowCount() {
            return entry.file().deleteRowCount();
        }

        @Override
        public byte[] embeddedIndex() {
            return entry.file().embeddedIndex();
        }

        @Override
        public BinaryRow minKey() {
            return entry.file().minKey();
        }

        @Override
        public BinaryRow maxKey() {
            return entry.file().maxKey();
        }

        @Override
        public SimpleStats keyStats() {
            return entry.file().keyStats();
        }

        @Override
        public SimpleStats valueStats() {
            return entry.file().valueStats();
        }

        @Override
        public long minSequenceNumber() {
            return entry.file().minSequenceNumber();
        }

        @Override
        public long maxSequenceNumber() {
            return entry.file().maxSequenceNumber();
        }

        @Override
        public long schemaId() {
            return entry.file().schemaId();
        }

        @Override
        public int level() {
            return entry.file().level();
        }

        @Override
        public List<String> extraFiles() {
            return entry.file().extraFiles();
        }

        @Override
        public Timestamp creationTime() {
            return entry.file().creationTime();
        }

        @Override
        public long creationTimeEpochMillis() {
            return entry.file().creationTimeEpochMillis();
        }

        @Override
        public String fileFormat() {
            return entry.file().fileFormat();
        }

        @Override
        public Optional<String> externalPath() {
            return entry.file().externalPath();
        }

        @Override
        public Optional<String> externalPathDir() {
            return entry.file().externalPathDir();
        }

        @Override
        public Optional<FileSource> fileSource() {
            return entry.file().fileSource();
        }

        @Override
        public @Nullable List<String> valueStatsCols() {
            return entry.file().valueStatsCols();
        }

        @Override
        public @Nullable Long firstRowId() {
            return entry.file().firstRowId();
        }

        @Override
        public @Nullable List<String> writeCols() {
            return entry.file().writeCols();
        }

        @Override
        public DataFileMeta upgrade(int newLevel) {
            return entry.file().upgrade(newLevel);
        }

        @Override
        public DataFileMeta rename(String newFileName) {
            return entry.file().rename(newFileName);
        }

        @Override
        public DataFileMeta copyWithoutStats() {
            return entry.file().copyWithoutStats();
        }

        @Override
        public DataFileMeta assignSequenceNumber(long minSequenceNumber, long maxSequenceNumber) {
            return entry.file().assignSequenceNumber(minSequenceNumber, maxSequenceNumber);
        }

        @Override
        public DataFileMeta assignFirstRowId(long firstRowId) {
            return entry.file().assignFirstRowId(firstRowId);
        }

        @Override
        public DataFileMeta copy(List<String> newExtraFiles) {
            return entry.file().copy(newExtraFiles);
        }

        @Override
        public DataFileMeta newExternalPath(String newExternalPath) {
            return entry.file().newExternalPath(newExternalPath);
        }

        @Override
        public DataFileMeta copy(byte[] newEmbeddedIndex) {
            return entry.file().copy(newEmbeddedIndex);
        }
    }
}
