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
import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.data.BinaryRow;
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
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.apache.paimon.CoreOptions.SCAN_BOUNDED_WATERMARK;
import static org.apache.paimon.CoreOptions.STREAM_SCAN_MODE;
import static org.apache.paimon.CoreOptions.StreamScanMode.FILE_MONITOR;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.newBytesType;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** A table to produce modified files for snapshots. */
@Experimental
public class FileMonitorTable implements DataTable, ReadonlyTable {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable wrapped;

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {
                        new BigIntType(false),
                        newBytesType(false),
                        new IntType(false),
                        newBytesType(false),
                        newBytesType(false)
                    },
                    new String[] {
                        "_SNAPSHOT_ID", "_PARTITION", "_BUCKET", "_BEFORE_FILES", "_DATA_FILES"
                    });

    public FileMonitorTable(FileStoreTable wrapped) {
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(STREAM_SCAN_MODE.key(), FILE_MONITOR.getValue());
        dynamicOptions.put(SCAN_BOUNDED_WATERMARK.key(), null);
        this.wrapped = wrapped.copy(dynamicOptions);
    }

    @Override
    public OptionalLong currentSnapshot() {
        return wrapped.currentSnapshot();
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
    public TagManager tagManager() {
        return wrapped.tagManager();
    }

    @Override
    public BranchManager branchManager() {
        return wrapped.branchManager();
    }

    @Override
    public String name() {
        return "__internal_file_monitor_" + wrapped.location().getName();
    }

    @Override
    public RowType rowType() {
        return ROW_TYPE;
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
    public DataTableScan newScan() {
        return wrapped.newScan();
    }

    @Override
    public StreamDataTableScan newStreamScan() {
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
    public FileMonitorTable copy(Map<String, String> dynamicOptions) {
        return new FileMonitorTable(wrapped.copy(dynamicOptions));
    }

    @Override
    public FileIO fileIO() {
        return wrapped.fileIO();
    }

    public static RowType getRowType() {
        return ROW_TYPE;
    }

    private static class BucketsRead implements InnerTableRead {

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

            FileChange change =
                    new FileChange(
                            dataSplit.snapshotId(),
                            dataSplit.partition(),
                            dataSplit.bucket(),
                            dataSplit.beforeFiles(),
                            dataSplit.dataFiles());

            return new IteratorRecordReader<>(Collections.singletonList(toRow(change)).iterator());
        }
    }

    public static InternalRow toRow(FileChange change) throws IOException {
        DataFileMetaSerializer fileSerializer = new DataFileMetaSerializer();
        return GenericRow.of(
                change.snapshotId(),
                serializeBinaryRow(change.partition()),
                change.bucket(),
                fileSerializer.serializeList(change.beforeFiles()),
                fileSerializer.serializeList(change.dataFiles()));
    }

    public static FileChange toFileChange(InternalRow row) throws IOException {
        DataFileMetaSerializer fileSerializer = new DataFileMetaSerializer();
        return new FileChange(
                row.getLong(0),
                deserializeBinaryRow(row.getBinary(1)),
                row.getInt(2),
                fileSerializer.deserializeList(row.getBinary(3)),
                fileSerializer.deserializeList(row.getBinary(4)));
    }

    /** Pojo to record of file change. */
    public static class FileChange {

        private final long snapshotId;
        private final BinaryRow partition;
        private final int bucket;
        private final List<DataFileMeta> beforeFiles;
        private final List<DataFileMeta> dataFiles;

        public FileChange(
                long snapshotId,
                BinaryRow partition,
                int bucket,
                List<DataFileMeta> beforeFiles,
                List<DataFileMeta> dataFiles) {
            this.snapshotId = snapshotId;
            this.partition = partition;
            this.bucket = bucket;
            this.beforeFiles = beforeFiles;
            this.dataFiles = dataFiles;
        }

        public long snapshotId() {
            return snapshotId;
        }

        public BinaryRow partition() {
            return partition;
        }

        public int bucket() {
            return bucket;
        }

        public List<DataFileMeta> beforeFiles() {
            return beforeFiles;
        }

        public List<DataFileMeta> dataFiles() {
            return dataFiles;
        }

        @Override
        public String toString() {
            return "FileChange{"
                    + "snapshotId="
                    + snapshotId
                    + ", partition="
                    + partition
                    + ", bucket="
                    + bucket
                    + ", beforeFiles="
                    + beforeFiles
                    + ", dataFiles="
                    + dataFiles
                    + '}';
        }
    }
}
