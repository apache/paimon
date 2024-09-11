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
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.EmptyRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.stats.Statistics;
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
import org.apache.paimon.utils.JsonSerdeUtil;
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
import java.util.Optional;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing statistic of table. */
public class StatisticTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String STATISTICS = "statistics";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "snapshot_id", new BigIntType(false)),
                            new DataField(1, "schema_id", new BigIntType(false)),
                            new DataField(2, "mergedRecordCount", new BigIntType(true)),
                            new DataField(3, "mergedRecordSize", new BigIntType(true)),
                            new DataField(2, "colstat", SerializationUtils.newStringType(true))));

    private final FileIO fileIO;
    private final Path location;

    private final FileStoreTable dataTable;

    public StatisticTable(FileStoreTable dataTable) {
        this(dataTable.fileIO(), dataTable.location(), dataTable);
    }

    public StatisticTable(FileIO fileIO, Path location, FileStoreTable dataTable) {
        this.fileIO = fileIO;
        this.location = location;
        this.dataTable = dataTable;
    }

    @Override
    public String name() {
        return dataTable.name() + SYSTEM_TABLE_SPLITTER + STATISTICS;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.singletonList("snapshot_id");
    }

    @Override
    public InnerTableScan newScan() {
        return new StatisticTable.StatisticScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new StatisticRead(fileIO, dataTable);
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new StatisticTable(fileIO, location, dataTable.copy(dynamicOptions));
    }

    private class StatisticScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () -> Collections.singletonList(new StatisticTable.StatisticSplit(location));
        }
    }

    private static class StatisticSplit extends SingletonSplit {

        private static final long serialVersionUID = 1L;

        private final Path location;

        private StatisticSplit(Path location) {
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
            StatisticTable.StatisticSplit that = (StatisticTable.StatisticSplit) o;
            return Objects.equals(location, that.location);
        }

        @Override
        public int hashCode() {
            return Objects.hash(location);
        }
    }

    private static class StatisticRead implements InnerTableRead {

        private final FileIO fileIO;
        private int[][] projection;

        private final FileStoreTable dataTable;

        public StatisticRead(FileIO fileIO, FileStoreTable dataTable) {
            this.fileIO = fileIO;
            this.dataTable = dataTable;
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
            if (!(split instanceof StatisticTable.StatisticSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }

            Optional<Statistics> statisticsOptional = dataTable.statistics();
            if (statisticsOptional.isPresent()) {
                Statistics statistics = statisticsOptional.get();
                Iterator<Statistics> statisticsIterator =
                        Collections.singletonList(statistics).iterator();
                Iterator<InternalRow> rows = Iterators.transform(statisticsIterator, this::toRow);
                if (projection != null) {
                    rows =
                            Iterators.transform(
                                    rows, row -> ProjectedRow.from(projection).replaceRow(row));
                }
                return new IteratorRecordReader<>(rows);
            } else {
                return new EmptyRecordReader<>();
            }
        }

        private InternalRow toRow(Statistics statistics) {
            return GenericRow.of(
                    statistics.snapshotId(),
                    statistics.schemaId(),
                    statistics.mergedRecordCount().getAsLong(),
                    statistics.mergedRecordSize().getAsLong(),
                    BinaryString.fromString(JsonSerdeUtil.toJson(statistics.colStats())));
        }
    }
}
