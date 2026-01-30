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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.metrics.CompactMetric;
import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.InPredicateVisitor;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LeafPredicateExtractor;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.Or;
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
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.CompactMetricsManager;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;
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
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing committing compaction metrics of table. */
public class CompactionMetricsTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String COMPACTS = "compacts";
    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "snapshot_id", new BigIntType(false)),
                            new DataField(1, "commit_time", new TimestampType(false, 3)),
                            new DataField(2, "compact_duration", new BigIntType(false)),
                            new DataField(3, "max_duration", new BigIntType(false)),
                            new DataField(4, "min_duration", new BigIntType(false)),
                            new DataField(5, "partitions", SerializationUtils.newStringType(false)),
                            new DataField(
                                    6, "compact_type", SerializationUtils.newStringType(false)),
                            new DataField(7, "identifier", new BigIntType(false)),
                            new DataField(
                                    8, "commit_user", SerializationUtils.newStringType(false))));

    private final FileIO fileIO;
    private final Path location;

    private final FileStoreTable dataTable;

    public CompactionMetricsTable(FileStoreTable dataTable) {
        this.fileIO = dataTable.fileIO();
        this.location = dataTable.location();
        this.dataTable = dataTable;
    }

    @Override
    public String name() {
        return location.getName() + SYSTEM_TABLE_SPLITTER + COMPACTS;
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
    public FileIO fileIO() {
        return dataTable.fileIO();
    }

    @Override
    public InnerTableScan newScan() {
        return new SnapshotsScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new CompactionsRead(fileIO);
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new CompactionMetricsTable(dataTable.copy(dynamicOptions));
    }

    private class SnapshotsScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            // do filter in read
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () -> Collections.singletonList(new CompactionsSplit(location));
        }
    }

    private static class CompactionsSplit extends SingletonSplit {

        private static final long serialVersionUID = 1L;

        private final Path location;

        private CompactionsSplit(Path location) {
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
            CompactionsSplit that = (CompactionsSplit) o;
            return Objects.equals(location, that.location);
        }

        @Override
        public int hashCode() {
            return Objects.hash(location);
        }
    }

    private class CompactionsRead implements InnerTableRead {

        private final FileIO fileIO;
        private RowType readType;
        private Optional<Long> optionalFilterSnapshotIdMax = Optional.empty();
        private Optional<Long> optionalFilterSnapshotIdMin = Optional.empty();
        private final List<Long> snapshotIds = new ArrayList<>();

        public CompactionsRead(FileIO fileIO) {
            this.fileIO = fileIO;
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            if (predicate == null) {
                return this;
            }

            String leafName = "snapshot_id";
            if (predicate instanceof CompoundPredicate) {
                CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
                List<Predicate> children = compoundPredicate.children();
                if ((compoundPredicate.function()) instanceof And) {
                    for (Predicate leaf : children) {
                        handleLeafPredicate(leaf, leafName);
                    }
                }

                // optimize for IN filter
                if ((compoundPredicate.function()) instanceof Or) {
                    InPredicateVisitor.extractInElements(predicate, leafName)
                            .ifPresent(
                                    leafs ->
                                            leafs.forEach(
                                                    leaf ->
                                                            snapshotIds.add(
                                                                    Long.parseLong(
                                                                            leaf.toString()))));
                }
            } else {
                handleLeafPredicate(predicate, leafName);
            }

            return this;
        }

        public void handleLeafPredicate(Predicate predicate, String leafName) {
            LeafPredicate compactionPred =
                    predicate.visit(LeafPredicateExtractor.INSTANCE).get(leafName);
            if (compactionPred != null) {
                if (compactionPred.function() instanceof Equal) {
                    optionalFilterSnapshotIdMin =
                            Optional.of((Long) compactionPred.literals().get(0));
                    optionalFilterSnapshotIdMax =
                            Optional.of((Long) compactionPred.literals().get(0));
                }

                if (compactionPred.function() instanceof GreaterThan) {
                    optionalFilterSnapshotIdMin =
                            Optional.of((Long) compactionPred.literals().get(0) + 1);
                }

                if (compactionPred.function() instanceof GreaterOrEqual) {
                    optionalFilterSnapshotIdMin =
                            Optional.of((Long) compactionPred.literals().get(0));
                }

                if (compactionPred.function() instanceof LessThan) {
                    optionalFilterSnapshotIdMax =
                            Optional.of((Long) compactionPred.literals().get(0) - 1);
                }

                if (compactionPred.function() instanceof LessOrEqual) {
                    optionalFilterSnapshotIdMax =
                            Optional.of((Long) compactionPred.literals().get(0));
                }
            }
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
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            if (!(split instanceof CompactionsSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            dataTable.fileIO();
            CompactMetricsManager compactMetricsManager = dataTable.store().compactMetricsManager();
            Iterator<CompactMetric> metrics = compactMetricsManager.metrics();
            Iterator<InternalRow> rows =
                    Iterators.transform(metrics, metric -> metric != null ? toRow(metric) : null);
            if (readType != null) {
                rows =
                        Iterators.transform(
                                rows,
                                row ->
                                        ProjectedRow.from(
                                                        readType, CompactionMetricsTable.TABLE_TYPE)
                                                .replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }

        private InternalRow toRow(CompactMetric metric) {
            return GenericRow.of(
                    metric.snapshotId(),
                    Timestamp.fromLocalDateTime(
                            LocalDateTime.ofInstant(
                                    Instant.ofEpochMilli(metric.commitTime()),
                                    ZoneId.systemDefault())),
                    metric.compactDuration(),
                    metric.compactMaxDuration(),
                    metric.compactMinDuration(),
                    BinaryString.fromString(metric.buckets()),
                    BinaryString.fromString(metric.compactType()),
                    metric.identifier(),
                    BinaryString.fromString(metric.commitUser()));
        }
    }
}
