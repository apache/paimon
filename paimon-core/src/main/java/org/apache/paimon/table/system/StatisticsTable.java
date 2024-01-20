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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.stats.StatisticsManager;
import org.apache.paimon.stats.Stats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing committing statistics of table. */
public class StatisticsTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;
    public static final String STATISTICS = "statistics";
    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "snapshot_id", new BigIntType(false)),
                            new DataField(1, "mergedRecordCount", new BigIntType(true)),
                            new DataField(2, "mergedRecordSize", new BigIntType(true)),
                            new DataField(3, "distinctCount", new BigIntType(true)),
                            new DataField(4, "min", new BigIntType(true)),
                            new DataField(5, "max", new BigIntType(true)),
                            new DataField(6, "nullCount", new BigIntType(true)),
                            new DataField(7, "avgLen", new BigIntType(true)),
                            new DataField(8, "maxLen", new BigIntType(true))));
    private final FileIO fileIO;
    private final Path location;
    private final FileStoreTable dataTable;

    public StatisticsTable(FileIO fileIO, Path location, FileStoreTable dataTable) {
        this.fileIO = fileIO;
        this.location = location;
        this.dataTable = dataTable;
    }

    @Override
    public String name() {
        return location.getName() + SYSTEM_TABLE_SPLITTER + STATISTICS;
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
        return new StatisticsScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new StatisticsRead(fileIO);
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new StatisticsTable(fileIO, location, dataTable.copy(dynamicOptions));
    }

    private class StatisticsScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () -> Collections.singletonList(new StatisticsSplit(fileIO, location));
        }
    }

    private static class StatisticsSplit implements Split {

        private static final long serialVersionUID = 1L;

        private final FileIO fileIO;
        private final Path location;

        private StatisticsSplit(FileIO fileIO, Path location) {
            this.fileIO = fileIO;
            this.location = location;
        }

        @Override
        public long rowCount() {
            try {
                return new StatisticsManager(fileIO, location).statisticsCount();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StatisticsSplit that = (StatisticsSplit) o;
            return Objects.equals(location, that.location);
        }

        @Override
        public int hashCode() {
            return Objects.hash(location);
        }
    }

    private static class StatisticsRead implements InnerTableRead {

        private final FileIO fileIO;
        private int[][] projection;

        public StatisticsRead(FileIO fileIO) {
            this.fileIO = fileIO;
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
            if (!(split instanceof StatisticsSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            Path location = ((StatisticsSplit) split).location;
            Iterator<Stats> statistics = new StatisticsManager(fileIO, location).statistics();
            Iterator<InternalRow> rows = Iterators.transform(statistics, this::toRow);
            if (projection != null) {
                rows =
                        Iterators.transform(
                                rows, row -> ProjectedRow.from(projection).replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }

        private InternalRow toRow(Stats statistic) {
            return GenericRow.of(
                    statistic.snapshotId(),
                    statistic.mergedRecordCount(),
                    statistic.mergedRecordSize(),
                    statistic.colStats().get("colId").colId(),
                    statistic.colStats().get("colId").distinctCount(),
                    statistic.colStats().get("colId").min(),
                    statistic.colStats().get("colId").max(),
                    statistic.colStats().get("colId").nullCount(),
                    statistic.colStats().get("colId").avgLen(),
                    statistic.colStats().get("colId").maxLen());
        }
    }
}
