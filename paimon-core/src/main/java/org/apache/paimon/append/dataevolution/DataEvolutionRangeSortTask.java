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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BlobDescriptorResolvingRow;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.TypeCheckUtils;
import org.apache.paimon.utils.UriReaderFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Rewrites all visible rows of complete file groups in one partition in sort-key order. */
public class DataEvolutionRangeSortTask extends DataEvolutionMaterializeDeletionCompactTask {

    private final List<String> sortFields;

    DataEvolutionRangeSortTask(
            BinaryRow partition,
            List<DataFileMeta> files,
            List<DeletionFile> deletions,
            List<String> sortFields) {
        super(partition, files, deletions);
        this.sortFields = Collections.unmodifiableList(new ArrayList<>(sortFields));
    }

    public static List<String> parseSortFields(String orderBy) {
        return Arrays.stream(orderBy.split(",", -1)).map(String::trim).collect(Collectors.toList());
    }

    public static void validate(FileStoreTable table, long start, long end, List<String> fields) {
        checkArgument(
                table.coreOptions().dataEvolutionEnabled(),
                "Range sorting requires a data evolution table.");
        checkArgument(
                start >= 0 && end >= start, "Invalid inclusive row ID range [%s, %s].", start, end);
        checkArgument(
                !fields.isEmpty() && new HashSet<>(fields).size() == fields.size(),
                "Specify distinct sort fields.");
        RowType rowType = table.rowType();
        for (String field : fields) {
            int index = rowType.getFieldIndex(field);
            checkArgument(
                    index >= 0 && TypeCheckUtils.isComparable(rowType.getTypeAt(index)),
                    "Expected a comparable top-level sort field, found '%s'.",
                    field);
        }
    }

    public static List<DataEvolutionCompactTask> plan(
            FileStoreTable table,
            Snapshot snapshot,
            Range range,
            @Nullable PartitionPredicate partitionFilter,
            List<String> sortFields) {
        validate(table, range.from, range.to, sortFields);
        checkArgument(
                table.schema().id() >= snapshot.schemaId(),
                "Table schema %s is older than the schema of planning snapshot %s (%s). Reload the table before sorting.",
                table.schema().id(),
                snapshot.id(),
                snapshot.schemaId());
        List<DataSplit> splits =
                table.newSnapshotReader()
                        .withSnapshot(snapshot)
                        .withPartitionFilter(partitionFilter)
                        .withRowRanges(Collections.singletonList(range))
                        .read()
                        .dataSplits();
        Map<BinaryRow, List<DataSplit>> partitions = new LinkedHashMap<>();
        for (DataSplit split : splits) {
            partitions.computeIfAbsent(split.partition(), ignored -> new ArrayList<>()).add(split);
        }
        List<DataEvolutionCompactTask> tasks = new ArrayList<>();
        for (Map.Entry<BinaryRow, List<DataSplit>> entry : partitions.entrySet()) {
            List<DataFileMeta> files = new ArrayList<>();
            List<DeletionFile> deletions = new ArrayList<>();
            for (DataSplit split : entry.getValue()) {
                for (DataFileMeta file : split.dataFiles()) {
                    Range actual = file.nonNullRowIdRange();
                    checkArgument(
                            range.from <= actual.from && actual.to <= range.to,
                            "Row ID range %s cuts file %s covering %s. Select complete file groups.",
                            range,
                            file.fileName(),
                            actual);
                    files.add(file);
                }
                deletions.addAll(
                        split.deletionFiles()
                                .orElseGet(
                                        () -> Collections.nCopies(split.dataFiles().size(), null)));
            }
            tasks.add(new DataEvolutionRangeSortTask(entry.getKey(), files, deletions, sortFields));
        }
        return tasks;
    }

    List<String> sortFields() {
        return sortFields;
    }

    @Override
    public TaskType type() {
        return TaskType.RANGE_SORT;
    }

    @Override
    protected FileStoreTable configureTable(FileStoreTable table) {
        Map<String, String> options = new HashMap<>(BLOB_COMPACT_READ_OPTIONS);
        // A replacement must fail if the source cannot be read in full.
        options.put(CoreOptions.SCAN_IGNORE_LOST_FILE.key(), "false");
        options.put(CoreOptions.SCAN_IGNORE_CORRUPT_FILE.key(), "false");
        options.put(CoreOptions.BLOB_WRITE_NULL_ON_MISSING_FILE.key(), "false");
        options.put(CoreOptions.BLOB_WRITE_NULL_ON_FETCH_FAILURE.key(), "false");
        options.put(CoreOptions.WRITE_BUFFER_FOR_APPEND.key(), "false");
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        return table.copyWithoutTimeTravel(options);
    }

    @Override
    protected RowType readType(FileStoreTable table) {
        return SpecialFields.rowTypeWithRowId(table.rowType());
    }

    @Override
    protected void writeRows(
            FileStoreTable table,
            RecordReader<InternalRow> reader,
            RecordWriter<InternalRow> writer,
            Supplier<IOManager> ioManagerFactory)
            throws Exception {
        RowType input = readType(table);
        int[] fields = Arrays.copyOf(input.getFieldIndices(sortFields), sortFields.size() + 1);
        fields[sortFields.size()] = input.getFieldIndex(SpecialFields.ROW_ID.name());
        CoreOptions options = table.coreOptions();
        try (IOManager io = ioManagerFactory.get()) {
            BinaryExternalSortBuffer sorter =
                    BinaryExternalSortBuffer.create(
                            io,
                            input,
                            fields,
                            options.writeBufferSize(),
                            options.pageSize(),
                            options.localSortMaxNumFileHandles(),
                            options.spillCompressOptions(),
                            options.writeBufferSpillDiskSize());
            sorter.enforceSpillDiskLimit();
            try {
                try (CloseableIterator<InternalRow> rows = reader.toCloseableIterator()) {
                    while (rows.hasNext()) {
                        sorter.write(rows.next());
                    }
                }
                // Managed payloads stay in the source packs until the sorted writer copies them.
                BlobDescriptorResolvingRow resolved =
                        new BlobDescriptorResolvingRow(
                                UriReaderFactory.fromFileIO(table.fileIO()), null);
                ProjectedRow output = ProjectedRow.from(table.rowType(), input);
                MutableObjectIterator<BinaryRow> sorted = sorter.sortedIterator();
                BinaryRow row = new BinaryRow(input.getFieldCount());
                while ((row = sorted.next(row)) != null) {
                    writer.write(output.replaceRow(resolved.replace(row)));
                }
            } finally {
                sorter.clear();
            }
        }
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other)
                && sortFields.equals(((DataEvolutionRangeSortTask) other).sortFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sortFields);
    }
}
