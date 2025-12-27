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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.DataEvolutionArray;
import org.apache.paimon.reader.DataEvolutionRow;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** {@link FileStoreScan} for data-evolution enabled table. */
public class DataEvolutionFileStoreScan extends AppendOnlyFileStoreScan {

    private boolean dropStats = false;
    @Nullable private RowType readType;

    public DataEvolutionFileStoreScan(
            ManifestsReader manifestsReader,
            BucketSelectConverter bucketSelectConverter,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            ManifestFile.Factory manifestFileFactory,
            Integer scanManifestParallelism,
            boolean deletionVectorsEnabled) {
        super(
                manifestsReader,
                bucketSelectConverter,
                snapshotManager,
                schemaManager,
                schema,
                manifestFileFactory,
                scanManifestParallelism,
                false,
                deletionVectorsEnabled);
    }

    @Override
    public FileStoreScan dropStats() {
        // overwrite to keep stats here
        // TODO refactor this hacky
        this.dropStats = true;
        return this;
    }

    @Override
    public FileStoreScan keepStats() {
        // overwrite to keep stats here
        // TODO refactor this hacky
        this.dropStats = false;
        return this;
    }

    @Override
    public DataEvolutionFileStoreScan withFilter(Predicate predicate) {
        // overwrite to keep all filter here
        // TODO refactor this hacky
        this.inputFilter = predicate;
        return this;
    }

    @Override
    public FileStoreScan withReadType(RowType readType) {
        if (readType != null) {
            List<DataField> nonSystemFields =
                    readType.getFields().stream()
                            .filter(f -> !SpecialFields.isSystemField(f.id()))
                            .collect(Collectors.toList());
            if (!nonSystemFields.isEmpty()) {
                this.readType = readType;
            }
        }
        return this;
    }

    @Override
    public boolean supportsLimitPushManifestEntries() {
        return false;
    }

    @Override
    protected boolean postFilterManifestEntriesEnabled() {
        return inputFilter != null;
    }

    @Override
    protected List<ManifestEntry> postFilterManifestEntries(List<ManifestEntry> entries) {
        checkNotNull(inputFilter);

        // group by row id range
        RangeHelper<ManifestEntry> rangeHelper =
                new RangeHelper<>(
                        e -> e.file().nonNullFirstRowId(),
                        e -> e.file().nonNullFirstRowId() + e.file().rowCount() - 1);
        List<List<ManifestEntry>> splitByRowId = rangeHelper.mergeOverlappingRanges(entries);

        return splitByRowId.stream()
                .filter(this::filterByStats)
                .flatMap(Collection::stream)
                .map(entry -> dropStats ? dropStats(entry) : entry)
                .collect(Collectors.toList());
    }

    private boolean filterByStats(List<ManifestEntry> entries) {
        EvolutionStats stats = evolutionStats(schema, this::scanTableSchema, entries);
        return inputFilter.test(
                stats.rowCount(), stats.minValues(), stats.maxValues(), stats.nullCounts());
    }

    /** TODO: Optimize implementation of this method. */
    @VisibleForTesting
    static EvolutionStats evolutionStats(
            TableSchema schema,
            Function<Long, TableSchema> scanTableSchema,
            List<ManifestEntry> metas) {
        // exclude blob files, useless for predicate eval
        metas =
                metas.stream()
                        .filter(entry -> !isBlobFile(entry.file().fileName()))
                        .collect(Collectors.toList());

        ToLongFunction<ManifestEntry> maxSeqFunc = e -> e.file().maxSequenceNumber();
        metas.sort(Comparator.comparingLong(maxSeqFunc).reversed());

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
            SimpleStats stats = metas.get(i).file().valueStats();
            min[i] = stats.minValues();
            max[i] = stats.maxValues();
            nullCounts[i] = stats.nullCounts();
        }

        for (int i = 0; i < metas.size(); i++) {
            DataFileMeta fileMeta = metas.get(i).file();

            TableSchema dataFileSchema =
                    scanTableSchema.apply(fileMeta.schemaId()).project(fileMeta.writeCols());

            TableSchema dataFileSchemaWithStats = dataFileSchema.project(fileMeta.valueStatsCols());

            int[] fieldIds =
                    dataFileSchema.logicalRowType().getFields().stream()
                            .mapToInt(DataField::id)
                            .toArray();

            int[] fieldIdsWithStats =
                    dataFileSchemaWithStats.logicalRowType().getFields().stream()
                            .mapToInt(DataField::id)
                            .toArray();

            loop1:
            for (int j = 0; j < fieldsCount; j++) {
                if (rowOffsets[j] != -1) {
                    continue;
                }
                int targetFieldId = allFields[j];
                for (int fieldId : fieldIds) {
                    if (targetFieldId == fieldId) {
                        for (int k = 0; k < fieldIdsWithStats.length; k++) {
                            if (fieldId == fieldIdsWithStats[k]) {
                                // TODO: If type not match (e.g. int -> string), we need to skip
                                // this, set rowOffsets[j] = -1 always. (may -2, after all, set it
                                // back to -1) Because schema evolution may happen to change int to
                                // string or something like that.
                                rowOffsets[j] = i;
                                fieldOffsets[j] = k;
                                continue loop1;
                            }
                        }
                        rowOffsets[j] = -2;
                        continue loop1;
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
        return new EvolutionStats(
                metas.get(0).file().rowCount(), finalMin, finalMax, finalNullCounts);
    }

    /** Note: Keep this thread-safe. */
    @Override
    protected boolean filterByStats(ManifestEntry entry) {
        DataFileMeta file = entry.file();

        if (readType != null) {
            boolean containsReadCol = false;
            RowType fileType =
                    scanTableSchema(file.schemaId()).project(file.writeCols()).logicalRowType();
            for (String field : readType.getFieldNames()) {
                if (fileType.containsField(field)) {
                    containsReadCol = true;
                    break;
                }
            }
            if (!containsReadCol) {
                return false;
            }
        }

        // If rowRanges is null, all entries should be kept
        if (this.rowRanges == null) {
            return true;
        }

        // If entry.firstRowId does not exist, keep the entry
        Long firstRowId = file.firstRowId();
        if (firstRowId == null) {
            return true;
        }

        // Check if any value in indices is in the range [firstRowId, firstRowId + rowCount - 1]
        long rowCount = file.rowCount();
        long endRowId = firstRowId + rowCount - 1;
        Range fileRowRange = new Range(firstRowId, endRowId);

        for (Range expected : rowRanges) {
            if (Range.intersection(fileRowRange, expected) != null) {
                return true;
            }
        }

        // No matching indices found, skip this entry
        return false;
    }

    /** Statistics for data evolution. */
    public static class EvolutionStats {

        private final long rowCount;
        private final InternalRow minValues;
        private final InternalRow maxValues;
        private final InternalArray nullCounts;

        public EvolutionStats(
                long rowCount,
                InternalRow minValues,
                InternalRow maxValues,
                InternalArray nullCounts) {
            this.rowCount = rowCount;
            this.minValues = minValues;
            this.maxValues = maxValues;
            this.nullCounts = nullCounts;
        }

        public long rowCount() {
            return rowCount;
        }

        public InternalRow minValues() {
            return minValues;
        }

        public InternalRow maxValues() {
            return maxValues;
        }

        public InternalArray nullCounts() {
            return nullCounts;
        }
    }
}
