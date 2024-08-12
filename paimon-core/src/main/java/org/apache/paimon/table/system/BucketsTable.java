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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.manifest.BucketEntry;
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
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

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
public class BucketsTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String BUCKETS = "buckets";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "bucket", new IntType(true)),
                            new DataField(1, "record_count", new BigIntType(false)),
                            new DataField(2, "file_size_in_bytes", new BigIntType(false)),
                            new DataField(3, "file_count", new BigIntType(false)),
                            new DataField(4, "level0_file_count", new BigIntType(false)),
                            new DataField(5, "level0_file_backlog_ratio", new DoubleType(false)),
                            new DataField(6, "last_update_time", DataTypes.TIMESTAMP_MILLIS())));

    private final FileStoreTable storeTable;

    public BucketsTable(FileStoreTable storeTable) {
        this.storeTable = storeTable;
    }

    @Override
    public String name() {
        return storeTable.name() + SYSTEM_TABLE_SPLITTER + BUCKETS;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.singletonList("buckets");
    }

    @Override
    public InnerTableScan newScan() {
        return new BucketsScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new BucketsRead(storeTable);
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new BucketsTable(storeTable.copy(dynamicOptions));
    }

    private static class BucketsScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () -> Collections.singletonList(new BucketsSplit());
        }
    }

    private static class BucketsSplit extends SingletonSplit {

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

    private static class BucketsRead implements InnerTableRead {

        private final FileStoreTable fileStoreTable;

        private int[][] projection;

        public BucketsRead(FileStoreTable table) {
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
        public RecordReader<InternalRow> createReader(Split split) {
            if (!(split instanceof BucketsSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }

            List<BucketEntry> partitions = fileStoreTable.newSnapshotReader().bucketEntries();

            List<InternalRow> results = new ArrayList<>(partitions.size());
            for (BucketEntry entry : partitions) {
                results.add(toRow(entry));
            }

            Iterator<InternalRow> iterator = results.iterator();
            if (projection != null) {
                iterator =
                        Iterators.transform(
                                iterator, row -> ProjectedRow.from(projection).replaceRow(row));
            }
            return new IteratorRecordReader<>(iterator);
        }

        private GenericRow toRow(BucketEntry entry) {
            return GenericRow.of(
                    entry.bucket(),
                    entry.recordCount(),
                    entry.fileSizeInBytes(),
                    entry.fileCount(),
                    entry.level0FileCount(),
                    (double) entry.level0FileCount() / entry.triggerNum(),
                    Timestamp.fromLocalDateTime(
                            LocalDateTime.ofInstant(
                                    Instant.ofEpochMilli(entry.lastFileCreationTime()),
                                    ZoneId.systemDefault())));
        }
    }
}
