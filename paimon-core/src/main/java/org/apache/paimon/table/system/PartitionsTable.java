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

package org.apache.paimon.table.system;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.LazyGenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing partitions info. */
public class PartitionsTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String PARTITIONS = "partitions";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "partition", SerializationUtils.newStringType(true)),
                            new DataField(1, "record_count", new BigIntType(false)),
                            new DataField(2, "file_size_in_bytes", new BigIntType(false)),
                            new DataField(3, "file_count", new BigIntType(false)),
                            new DataField(4, "last_update_time", DataTypes.TIMESTAMP_MILLIS())));

    private final FileStoreTable storeTable;

    public PartitionsTable(FileStoreTable storeTable) {
        this.storeTable = storeTable;
    }

    @Override
    public String name() {
        return storeTable.name() + SYSTEM_TABLE_SPLITTER + PARTITIONS;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.singletonList("partition");
    }

    @Override
    public InnerTableScan newScan() {
        return new PartitionsScan(storeTable);
    }

    @Override
    public InnerTableRead newRead() {
        return new PartitionsRead(storeTable);
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new PartitionsTable(storeTable.copy(dynamicOptions));
    }

    private static class PartitionsScan extends ReadOnceTableScan {

        private final FileStoreTable storeTable;

        private PartitionsScan(FileStoreTable storeTable) {
            this.storeTable = storeTable;
        }

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () ->
                    Collections.singletonList(
                            new PartitionsSplit(storeTable.newScan().plan().splits()));
        }
    }

    private static class PartitionsSplit implements Split {

        private static final long serialVersionUID = 1L;

        private final List<Split> splits;

        private PartitionsSplit(List<Split> splits) {
            this.splits = splits;
        }

        @Override
        public long rowCount() {
            return splits.stream()
                    .map(s -> ((DataSplit) s).partition())
                    .collect(Collectors.toSet())
                    .size();
        }

        private List<Split> splits() {
            return splits;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PartitionsSplit that = (PartitionsSplit) o;
            return Objects.equals(splits, that.splits);
        }

        @Override
        public int hashCode() {
            return Objects.hash(splits);
        }
    }

    private static class PartitionsRead implements InnerTableRead {

        private final FileStoreTable fileStoreTable;

        private int[][] projection;

        public PartitionsRead(FileStoreTable table) {
            this.fileStoreTable = table;
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        @Override
        public InnerTableRead withProjection(int[][] projection) {
            this.projection = projection;
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            if (!(split instanceof PartitionsSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            PartitionsSplit filesSplit = (PartitionsSplit) split;
            if (filesSplit.splits().isEmpty()) {
                return new IteratorRecordReader<>(Collections.emptyIterator());
            }
            List<Iterator<InternalRow>> iteratorList = new ArrayList<>();
            RowDataToObjectArrayConverter partitionConverter =
                    new RowDataToObjectArrayConverter(
                            fileStoreTable.schema().logicalPartitionType());

            for (Split dataSplit : filesSplit.splits()) {
                iteratorList.add(
                        Iterators.transform(
                                ((DataSplit) dataSplit).dataFiles().iterator(),
                                file -> toRow((DataSplit) dataSplit, partitionConverter, file)));
            }
            Iterator<InternalRow> rows = Iterators.concat(iteratorList.iterator());
            // Group by partition and sum the others
            Iterator<InternalRow> resultRows = groupAndSum(rows);

            if (projection != null) {
                resultRows =
                        Iterators.transform(
                                resultRows, row -> ProjectedRow.from(projection).replaceRow(row));
            }

            return new IteratorRecordReader<>(resultRows);
        }

        private LazyGenericRow toRow(
                DataSplit dataSplit,
                RowDataToObjectArrayConverter partitionConverter,
                DataFileMeta dataFileMeta) {

            BinaryString partitionId =
                    dataSplit.partition() == null
                            ? null
                            : BinaryString.fromString(
                                    Arrays.toString(
                                            partitionConverter.convert(dataSplit.partition())));
            @SuppressWarnings("unchecked")
            Supplier<Object>[] fields =
                    new Supplier[] {
                        () -> partitionId,
                        dataFileMeta::rowCount,
                        dataFileMeta::fileSize,
                        dataFileMeta::creationTimeEpochMillis
                    };

            return new LazyGenericRow(fields);
        }
    }

    public static Iterator<InternalRow> groupAndSum(Iterator<InternalRow> rows) {
        return new GroupedIterator(rows);
    }

    /** group by partition and sum the recordCount and fileBytes . */
    static class GroupedIterator implements Iterator<InternalRow> {
        private final Iterator<InternalRow> rows;
        private final Map<BinaryString, Partition> groupedData;
        private Iterator<Partition> resultIterator;

        public GroupedIterator(Iterator<InternalRow> rows) {
            this.rows = rows;
            this.groupedData = new HashMap<>();
            groupAndSum();
        }

        private void groupAndSum() {
            while (rows.hasNext()) {
                InternalRow row = rows.next();
                BinaryString partitionId = row.getString(0);
                long recordCount = row.getLong(1);
                long fileSizeInBytes = row.getLong(2);
                long lastFileCreationTime = row.getLong(3);

                // Grouping and summing
                Partition rowData =
                        groupedData.computeIfAbsent(
                                partitionId, key -> new Partition(partitionId, 0, 0, 0, -1));
                rowData.recordCount += recordCount;
                rowData.fileSizeInBytes += fileSizeInBytes;
                rowData.fileCount++;
                rowData.lastFileCreationTime =
                        Math.max(rowData.lastFileCreationTime, lastFileCreationTime);
            }
            resultIterator = groupedData.values().iterator();
        }

        @Override
        public boolean hasNext() {
            return resultIterator.hasNext();
        }

        @Override
        public InternalRow next() {
            if (hasNext()) {
                Partition partition = resultIterator.next();
                return GenericRow.of(
                        partition.partition,
                        partition.recordCount,
                        partition.fileSizeInBytes,
                        partition.fileCount,
                        Timestamp.fromLocalDateTime(
                                LocalDateTime.ofInstant(
                                        Instant.ofEpochMilli(partition.lastFileCreationTime),
                                        ZoneId.systemDefault())));
            } else {
                throw new NoSuchElementException("No more elements in the iterator.");
            }
        }
    }

    static class Partition {
        private final BinaryString partition;
        private long recordCount;
        private long fileSizeInBytes;

        private long fileCount;

        private long lastFileCreationTime;

        Partition(
                BinaryString partition,
                long recordCount,
                long fileSizeInBytes,
                long fileCount,
                long lastFileCreationTime) {
            this.partition = partition;
            this.recordCount = recordCount;
            this.fileSizeInBytes = fileSizeInBytes;
            this.lastFileCreationTime = lastFileCreationTime;
            this.fileCount = fileCount;
        }

        public long recordCount() {
            return recordCount;
        }

        public long fileSize() {
            return fileSizeInBytes;
        }

        public long getFileCount() {
            return fileCount;
        }

        public long getLastFileCreationTime() {
            return lastFileCreationTime;
        }
    }
}
