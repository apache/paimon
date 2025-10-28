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
import org.apache.paimon.stats.SimpleStatsEvolution;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.source.DataEvolutionSplitGenerator;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/** {@link FileStoreScan} for data-evolution enabled table. */
public class DataEvolutionFileStoreScan extends AppendOnlyFileStoreScan {

    private boolean dropStats = false;
    @Nullable private List<Long> indices;

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

    @Override
    public FileStoreScan dropStats() {
        this.dropStats = true;
        return this;
    }

    @Override
    public FileStoreScan keepStats() {
        this.dropStats = false;
        return this;
    }

    public DataEvolutionFileStoreScan withFilter(Predicate predicate) {
        this.inputFilter = predicate;
        return this;
    }

    @Override
    public FileStoreScan withRowIds(List<Long> indices) {
        this.indices = indices;
        return this;
    }

    @Override
    protected List<ManifestEntry> postFilter(List<ManifestEntry> entries) {
        if (inputFilter == null) {
            return entries;
        }
        List<List<ManifestEntry>> splitByRowId =
                DataEvolutionSplitGenerator.splitManifests(entries);

        return splitByRowId.stream()
                .filter(this::filterByStats)
                .flatMap(Collection::stream)
                .map(entry -> dropStats ? dropStats(entry) : entry)
                .collect(Collectors.toList());
    }

    private boolean filterByStats(List<ManifestEntry> metas) {
        long rowCount = metas.get(0).file().rowCount();
        SimpleStatsEvolution.Result evolutionResult = evolutionStats(metas);
        return inputFilter.test(
                rowCount,
                evolutionResult.minValues(),
                evolutionResult.maxValues(),
                evolutionResult.nullCounts());
    }

    private SimpleStatsEvolution.Result evolutionStats(List<ManifestEntry> metas) {
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
        // If indices is null, all entries should be kept
        if (this.indices == null) {
            return true;
        }

        // If entry.firstRowId does not exist, keep the entry
        Long firstRowId = entry.file().firstRowId();
        if (firstRowId == null) {
            return true;
        }

        // Check if any value in indices is in the range [firstRowId, firstRowId + rowCount)
        long rowCount = entry.file().rowCount();
        long endRowId = firstRowId + rowCount;

        for (Long index : this.indices) {
            if (index >= firstRowId && index < endRowId) {
                return true;
            }
        }

        // No matching indices found, skip this entry
        return false;
    }
}
