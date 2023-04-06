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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerStreamTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.snapshot.SnapshotSplitReader;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.SnapshotManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A table to produce modified partitions and buckets for each snapshot.
 *
 * <p>Only used internally by dedicated compact job sources.
 */
public class BucketsTable implements DataTable, ReadonlyTable {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable wrapped;
    private final boolean isContinuous;

    public BucketsTable(FileStoreTable wrapped, boolean isContinuous) {
        this.wrapped = wrapped;
        this.isContinuous = isContinuous;
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

        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_SNAPSHOT_ID", new BigIntType()));
        fields.addAll(partitionType.getFields());
        // same with ManifestEntry.schema
        fields.add(new DataField(1, "_BUCKET", new IntType()));
        fields.add(new DataField(2, "_FILES", new VarBinaryType()));
        return new RowType(fields);
    }

    public static RowType partitionWithBucketRowType(RowType partitionType) {
        List<DataField> fields = new ArrayList<>(partitionType.getFields());
        // same with ManifestEntry.schema
        fields.add(new DataField(3, "_BUCKET", new IntType()));
        return new RowType(fields);
    }

    @Override
    public Map<String, String> options() {
        return wrapped.options();
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.emptyList();
    }

    @Override
    public SnapshotSplitReader newSnapshotSplitReader() {
        return wrapped.newSnapshotSplitReader();
    }

    @Override
    public InnerTableScan newScan() {
        return wrapped.newScan();
    }

    @Override
    public InnerStreamTableScan newStreamScan() {
        return wrapped.newStreamScan();
    }

    @Override
    public CoreOptions coreOptions() {
        return wrapped.coreOptions();
    }

    @Override
    public InnerTableRead newRead() {
        return new BucketsRead();
    }

    @Override
    public BucketsTable copy(Map<String, String> dynamicOptions) {
        return new BucketsTable(wrapped.copy(dynamicOptions), isContinuous);
    }

    @Override
    public FileIO fileIO() {
        return wrapped.fileIO();
    }

    private class BucketsRead implements InnerTableRead {

        private final DataFileMetaSerializer dataFileMetaSerializer = new DataFileMetaSerializer();

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            // filter is done by scan
            return this;
        }

        @Override
        public InnerTableRead withProjection(int[][] projection) {
            throw new UnsupportedOperationException("BucketsRead does not support projection");
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            if (!(split instanceof DataSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }

            DataSplit dataSplit = (DataSplit) split;

            InternalRow row =
                    new JoinedRow(GenericRow.of(dataSplit.snapshotId()), dataSplit.partition());
            row = new JoinedRow(row, GenericRow.of(dataSplit.bucket()));

            List<DataFileMeta> files = Collections.emptyList();
            if (isContinuous) {
                // Serialized files are only useful in streaming jobs.
                // Batch compact jobs only run once, so they only need to know what buckets should
                // be compacted and don't need to concern incremental new files.
                files = dataSplit.files();
            }
            row =
                    new JoinedRow(
                            row,
                            GenericRow.of((Object) dataFileMetaSerializer.serializeList(files)));

            return new IteratorRecordReader<>(Collections.singletonList(row).iterator());
        }
    }
}
