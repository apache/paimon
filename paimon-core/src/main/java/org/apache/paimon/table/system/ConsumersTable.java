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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
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
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing consumers of table. */
public class ConsumersTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String CONSUMERS = "consumers";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(
                                    0, "consumer_id", SerializationUtils.newStringType(false)),
                            new DataField(1, "next_snapshot_id", new BigIntType(false))));

    private final FileIO fileIO;
    private final Path location;
    private final String branch;

    public ConsumersTable(FileStoreTable dataTable) {
        this(
                dataTable.fileIO(),
                dataTable.location(),
                CoreOptions.branch(dataTable.schema().options()));
    }

    public ConsumersTable(FileIO fileIO, Path location, String branchName) {
        this.fileIO = fileIO;
        this.location = location;
        this.branch = branchName;
    }

    @Override
    public String name() {
        return location.getName() + SYSTEM_TABLE_SPLITTER + CONSUMERS;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.singletonList("consumer_id");
    }

    @Override
    public InnerTableScan newScan() {
        return new ConsumersTable.ConsumersScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new ConsumersTable.ConsumersRead(fileIO);
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new ConsumersTable(fileIO, location, branch);
    }

    private class ConsumersScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () -> Collections.singletonList(new ConsumersTable.ConsumersSplit(location));
        }
    }

    /** {@link Split} implementation for {@link ConsumersTable}. */
    private static class ConsumersSplit extends SingletonSplit {

        private static final long serialVersionUID = 1L;

        private final Path location;

        private ConsumersSplit(Path location) {
            this.location = location;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ConsumersTable.ConsumersSplit that = (ConsumersTable.ConsumersSplit) o;
            return Objects.equals(location, that.location);
        }

        @Override
        public int hashCode() {
            return Objects.hash(location);
        }
    }

    /** {@link TableRead} implementation for {@link ConsumersTable}. */
    private class ConsumersRead implements InnerTableRead {

        private final FileIO fileIO;
        private int[][] projection;

        public ConsumersRead(FileIO fileIO) {
            this.fileIO = fileIO;
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
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
            if (!(split instanceof ConsumersTable.ConsumersSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            Path location = ((ConsumersTable.ConsumersSplit) split).location;
            Map<String, Long> consumers = new ConsumerManager(fileIO, location, branch).consumers();
            Iterator<InternalRow> rows =
                    Iterators.transform(consumers.entrySet().iterator(), this::toRow);
            if (projection != null) {
                rows =
                        Iterators.transform(
                                rows, row -> ProjectedRow.from(projection).replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }

        private InternalRow toRow(Map.Entry<String, Long> consumer) {
            return GenericRow.of(BinaryString.fromString(consumer.getKey()), consumer.getValue());
        }
    }
}
