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

package org.apache.flink.table.store.table.system;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.utils.IteratorRecordReader;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.DataTable;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.Table;
import org.apache.flink.table.store.table.source.DataSplit;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A table to produce modified partitions and buckets for each snapshot.
 *
 * <p>Only used internally by stand-alone compact job sources.
 */
public class BucketsTable implements DataTable {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable wrapped;

    public BucketsTable(FileStoreTable wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public Path location() {
        return wrapped.location();
    }

    @Override
    public SnapshotManager snapshotManager() {
        return wrapped.snapshotManager();
    }

    @Override
    public String name() {
        return "__internal_buckets_" + wrapped.location().getName();
    }

    @Override
    public RowType rowType() {
        RowType partitionType = wrapped.schema().logicalPartitionType();
        return rowType(partitionType);
    }

    public static RowType rowType(RowType partitionType) {
        List<RowType.RowField> fields = new ArrayList<>(partitionType.getFields());
        // same with ManifestEntry.schema
        fields.add(new RowType.RowField("_BUCKET", new IntType()));
        return new RowType(fields);
    }

    @Override
    public DataTableScan newScan() {
        return wrapped.newScan();
    }

    @Override
    public CoreOptions options() {
        return wrapped.options();
    }

    @Override
    public TableRead newRead() {
        return new BucketsRead();
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new BucketsTable(wrapped.copy(dynamicOptions));
    }

    private static class BucketsRead implements TableRead {

        @Override
        public TableRead withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public TableRead withProjection(int[][] projection) {
            throw new UnsupportedOperationException("BucketsRead does not support projection");
        }

        @Override
        public RecordReader<RowData> createReader(Split split) throws IOException {
            if (!(split instanceof DataSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }

            DataSplit dataSplit = (DataSplit) split;
            RowData row =
                    new JoinedRowData()
                            .replace(dataSplit.partition(), GenericRowData.of(dataSplit.bucket()));
            return new IteratorRecordReader<>(Collections.singletonList(row).iterator());
        }
    }
}
