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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.SingletonSplit;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing partitions info. */
public class PartitionsTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String PARTITIONS = "partitions";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "partition", SerializationUtils.newStringType(true)),
                            new DataField(1, "buckets", new BigIntType(false)),
                            new DataField(2, "record_count", new BigIntType(false)),
                            new DataField(3, "file_size_in_bytes", new BigIntType(false)),
                            new DataField(4, "file_count", new BigIntType(false)),
                            new DataField(5, "last_update_time", DataTypes.TIMESTAMP_MILLIS())));

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
        return new PartitionsScan();
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

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () -> Collections.singletonList(new PartitionsSplit());
        }
    }

    private static class PartitionsSplit extends SingletonSplit {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o != null && getClass() == o.getClass();
        }

        @Override
        public int hashCode() {
            return 1;
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

            List<PartitionEntry> partitions = fileStoreTable.newSnapshotReader().partitionEntries();

            RowDataToObjectArrayConverter converter =
                    new RowDataToObjectArrayConverter(
                            fileStoreTable.schema().logicalPartitionType());

            List<InternalRow> results = new ArrayList<>(partitions.size());
            for (PartitionEntry entry : partitions) {
                results.add(toRow(entry, converter));
            }

            Iterator<InternalRow> iterator = results.iterator();
            if (projection != null) {
                iterator =
                        Iterators.transform(
                                iterator, row -> ProjectedRow.from(projection).replaceRow(row));
            }
            return new IteratorRecordReader<>(iterator);
        }

        private GenericRow toRow(
                PartitionEntry entry, RowDataToObjectArrayConverter partitionConverter) {
            BinaryString partitionId =
                    BinaryString.fromString(
                            Arrays.toString(partitionConverter.convert(entry.partition())));
            return GenericRow.of(
                    partitionId,
                    entry.buckets(),
                    entry.recordCount(),
                    entry.fileSizeInBytes(),
                    entry.fileCount(),
                    Timestamp.fromLocalDateTime(
                            LocalDateTime.ofInstant(
                                    Instant.ofEpochMilli(entry.lastFileCreationTime()),
                                    ZoneId.systemDefault())));
        }
    }
}
