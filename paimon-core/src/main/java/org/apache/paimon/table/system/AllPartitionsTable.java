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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.SingletonSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** This is a system table to display all the database-table-partitions. */
public class AllPartitionsTable implements ReadonlyTable {

    public static final String ALL_PARTITIONS = "partitions";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "database_name", DataTypes.STRING()),
                            new DataField(1, "table_name", DataTypes.STRING()),
                            new DataField(2, "partition_name", DataTypes.STRING()),
                            new DataField(3, "record_count", DataTypes.BIGINT()),
                            new DataField(4, "file_size_in_bytes", DataTypes.BIGINT()),
                            new DataField(5, "file_count", DataTypes.BIGINT()),
                            new DataField(6, "last_file_creation_time", DataTypes.BIGINT()),
                            new DataField(7, "done", DataTypes.BOOLEAN())));

    private final List<GenericRow> rows;

    public AllPartitionsTable(List<GenericRow> rows) {
        this.rows = rows;
    }

    public static AllPartitionsTable fromPartitions(
            Map<Identifier, List<Partition>> allPartitions) {
        List<GenericRow> rows = new ArrayList<>();
        allPartitions.forEach(
                (identifier, partitions) ->
                        partitions.forEach(
                                partition -> {
                                    Map<String, String> spec = partition.spec();
                                    String partitionName = PartitionUtils.buildPartitionName(spec);
                                    GenericRow row =
                                            GenericRow.of(
                                                    BinaryString.fromString(
                                                            identifier.getDatabaseName()),
                                                    BinaryString.fromString(
                                                            identifier.getObjectName()),
                                                    BinaryString.fromString(partitionName),
                                                    partition.recordCount(),
                                                    partition.fileSizeInBytes(),
                                                    partition.fileCount(),
                                                    partition.lastFileCreationTime(),
                                                    partition.done());
                                    rows.add(row);
                                }));
        return new AllPartitionsTable(rows);
    }

    @Override
    public String name() {
        return ALL_PARTITIONS;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Arrays.asList(
                TABLE_TYPE.getField(0).name(),
                TABLE_TYPE.getField(1).name(),
                TABLE_TYPE.getField(2).name());
    }

    @Override
    public FileIO fileIO() {
        // pass a useless file io, should never use this.
        return new LocalFileIO();
    }

    @Override
    public InnerTableScan newScan() {
        return new AllPartitionsScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new AllPartitionsRead();
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new AllPartitionsTable(rows);
    }

    private class AllPartitionsScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () -> Collections.singletonList(new AllPartitionsSplit(rows));
        }
    }

    private static class AllPartitionsSplit extends SingletonSplit {

        private static final long serialVersionUID = 1L;

        private final List<GenericRow> rows;

        private AllPartitionsSplit(List<GenericRow> rows) {
            this.rows = rows;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AllPartitionsSplit that = (AllPartitionsSplit) o;
            return Objects.equals(rows, that.rows);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rows);
        }
    }

    private static class AllPartitionsRead implements InnerTableRead {

        private RowType readType;

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            this.readType = readType;
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) {
            if (!(split instanceof AllPartitionsSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            List<? extends InternalRow> rows = ((AllPartitionsSplit) split).rows;
            Iterator<? extends InternalRow> iterator = rows.iterator();
            if (readType != null) {
                iterator =
                        Iterators.transform(
                                iterator,
                                row ->
                                        ProjectedRow.from(
                                                        readType, AggregationFieldsTable.TABLE_TYPE)
                                                .replaceRow(row));
            }
            //noinspection ReassignedVariable,unchecked
            return new IteratorRecordReader<>((Iterator<InternalRow>) iterator);
        }
    }
}
