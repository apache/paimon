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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
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
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.SerializationUtils.newBytesType;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/**
 * A table to produce modified partitions and buckets for each snapshot.
 *
 * <p>Only used internally by dedicated compact job sources.
 */
public class BucketsMultiTable implements DataTable, ReadonlyTable {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable wrapped;
    private final boolean isContinuous;

    private final String databaseName;
    private final String tableName;

    public BucketsMultiTable(
            FileStoreTable wrapped, boolean isContinuous, String databaseName, String tableName) {
        this.wrapped = wrapped;
        this.isContinuous = isContinuous;
        this.databaseName = databaseName;
        this.tableName = tableName;
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
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_SNAPSHOT_ID", new BigIntType(false)));
        fields.add(new DataField(1, "_PARTITION", newBytesType(false)));
        fields.add(new DataField(2, "_BUCKET", new IntType(false)));
        fields.add(new DataField(3, "_FILES", newBytesType(false)));
        fields.add(new DataField(4, "_DATABASE_NAME", new VarCharType(Integer.MAX_VALUE)));
        fields.add(new DataField(5, "_TABLE_NAME", new VarCharType(Integer.MAX_VALUE)));
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
    public SnapshotReader newSnapshotReader() {
        return wrapped.newSnapshotReader();
    }

    @Override
    public TagManager tagManager() {
        return wrapped.tagManager();
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
        return new BucketsMultiTable.BucketsRead2();
    }

    @Override
    public BucketsMultiTable copy(Map<String, String> dynamicOptions) {
        return new BucketsMultiTable(
                wrapped.copy(dynamicOptions), isContinuous, databaseName, tableName);
    }

    @Override
    public FileIO fileIO() {
        return wrapped.fileIO();
    }

    public static RowType getRowType() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_SNAPSHOT_ID", new BigIntType(false)));
        fields.add(new DataField(1, "_PARTITION", newBytesType(false)));
        fields.add(new DataField(2, "_BUCKET", new IntType(false)));
        fields.add(new DataField(3, "_FILES", newBytesType(false)));
        fields.add(new DataField(4, "_DATABASE_NAME", new VarCharType(Integer.MAX_VALUE)));
        fields.add(new DataField(5, "_TABLE_NAME", new VarCharType(Integer.MAX_VALUE)));
        return new RowType(fields);
    }

    private class BucketsRead2 implements InnerTableRead {

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
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            if (!(split instanceof DataSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }

            DataSplit dataSplit = (DataSplit) split;

            List<DataFileMeta> files = Collections.emptyList();
            if (isContinuous) {
                // Serialized files are only useful in streaming jobs.
                // Batch compact jobs only run once, so they only need to know what buckets should
                // be compacted and don't need to concern incremental new files.
                files = dataSplit.dataFiles();
            }
            InternalRow row =
                    GenericRow.of(
                            dataSplit.snapshotId(),
                            serializeBinaryRow(dataSplit.partition()),
                            dataSplit.bucket(),
                            dataFileMetaSerializer.serializeList(files),
                            BinaryString.fromString(databaseName),
                            BinaryString.fromString(tableName));

            return new IteratorRecordReader<>(Collections.singletonList(row).iterator());
        }
    }
}
