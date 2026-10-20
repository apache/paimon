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

package org.apache.paimon.flink.cluster;

import org.apache.paimon.append.dataevolution.ClusteringRowIdMapping;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.memory.FlinkMemorySegmentPool;
import org.apache.paimon.flink.memory.MemorySegmentAllocator;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.ProjectedRow;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_USE_MANAGED_MEMORY;
import static org.apache.paimon.flink.utils.ManagedMemoryUtils.computeManagedMemory;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Writes sorted rows and records their original row IDs in output-file order. */
public class DataEvolutionClusterWriteOperator
        extends BoundedOneInputOperator<InternalRow, Committable> {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;
    private final BinaryRow partition;
    private final String commitUser;

    private transient IOManager ioManager;
    private transient BatchTableWrite write;
    private transient ProjectedRow valueProjection;
    private transient File rowIdSpool;
    private transient DataOutputStream rowIds;
    private transient List<Path> mappingPaths;
    private transient List<Path> preparedDataPaths;
    private transient MemorySegmentAllocator memoryAllocator;
    private transient boolean emitted;

    public DataEvolutionClusterWriteOperator(
            FileStoreTable table, BinaryRow partition, String commitUser) {
        this.table = table;
        this.partition = partition;
        this.commitUser = commitUser;
    }

    @Override
    public void open() throws Exception {
        super.open();
        ioManager =
                IOManager.create(
                        getContainingTask()
                                .getEnvironment()
                                .getIOManager()
                                .getSpillingDirectoriesPaths());
        rowIdSpool = ioManager.createChannel("cluster-row-ids").getPathFile();
        rowIds = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(rowIdSpool)));
        mappingPaths = new ArrayList<>();
        preparedDataPaths = new ArrayList<>();
        valueProjection =
                ProjectedRow.from(IntStream.range(0, table.rowType().getFieldCount()).toArray());
        // Unaware append writers use NoopCompactManager. Their data files, including files
        // produced by buffer flushes or spills, retain each partition's input order.
        write = table.newWrite(commitUser).withIOManager(ioManager);
        MemorySegmentPool memoryPool;
        if (Options.fromMap(table.options()).get(SINK_USE_MANAGED_MEMORY)) {
            MemoryManager memoryManager = getContainingTask().getEnvironment().getMemoryManager();
            memoryAllocator = new MemorySegmentAllocator(this, memoryManager);
            memoryPool =
                    new FlinkMemorySegmentPool(
                            computeManagedMemory(this),
                            memoryManager.getPageSize(),
                            memoryAllocator);
        } else {
            memoryPool =
                    new HeapMemorySegmentPool(
                            table.coreOptions().writeBufferSize(), table.coreOptions().pageSize());
        }
        write.withMemoryPoolFactory(new MemoryPoolFactory(memoryPool));
    }

    @Override
    public void processElement(StreamRecord<InternalRow> element) throws Exception {
        InternalRow row = element.getValue();
        int rowIdPosition = table.rowType().getFieldCount();
        checkArgument(!row.isNullAt(rowIdPosition), "Clustering source row ID must not be null.");
        long oldRowId = row.getLong(rowIdPosition);
        InternalRow value = valueProjection.replaceRow(row);
        checkArgument(
                write.getPartition(value).equals(partition),
                "Clustering writer received a row from another partition.");
        write.write(value, 0);
        rowIds.writeLong(oldRowId);
    }

    @Override
    public void endInput() throws Exception {
        rowIds.close();
        rowIds = null;
        List<CommitMessage> messages = write.prepareCommit();
        // prepareCommit transfers these files out of the writer's cleanup ownership. Keep them
        // until all mapping files and committables have been produced successfully.
        for (CommitMessage message : messages) {
            DataFilePathFactory preparedPathFactory =
                    table.store()
                            .pathFactory()
                            .createDataFilePathFactory(message.partition(), message.bucket());
            for (DataFileMeta file : ((CommitMessageImpl) message).newFilesIncrement().newFiles()) {
                preparedDataPaths.addAll(file.collectFiles(preparedPathFactory));
            }
        }
        DataFilePathFactory pathFactory =
                table.store().pathFactory().createDataFilePathFactory(partition, 0);
        List<Committable> committables = new ArrayList<>();
        try (DataInputStream input =
                new DataInputStream(new BufferedInputStream(new FileInputStream(rowIdSpool)))) {
            for (CommitMessage message : messages) {
                CommitMessageImpl impl = (CommitMessageImpl) message;
                DataIncrement increment = impl.newFilesIncrement();
                checkState(
                        message.partition().equals(partition)
                                && message.bucket() == 0
                                && impl.compactIncrement().isEmpty()
                                && increment.deletedFiles().isEmpty()
                                && increment.changelogFiles().isEmpty()
                                && increment.newIndexFiles().isEmpty()
                                && increment.deletedIndexFiles().isEmpty(),
                        "Clustering writer must only append files for its partition.");
                List<DataFileMeta> files = new ArrayList<>();
                for (DataFileMeta file : increment.newFiles()) {
                    checkState(
                            file.firstRowId() == null, "Clustering output must get new row IDs.");
                    String mappingName = ClusteringRowIdMapping.fileName(file.fileName());
                    Path mappingPath = pathFactory.toAlignedPath(mappingName, file);
                    mappingPaths.add(mappingPath);
                    try (ClusteringRowIdMapping.Writer mapping =
                            ClusteringRowIdMapping.createWriter(table.fileIO(), mappingPath)) {
                        for (long i = 0; i < file.rowCount(); i++) {
                            mapping.write(input.readLong());
                        }
                    }
                    List<String> extraFiles = new ArrayList<>(file.extraFiles());
                    extraFiles.add(mappingName);
                    files.add(file.copy(extraFiles));
                }
                committables.add(
                        new Committable(
                                Long.MAX_VALUE,
                                new CommitMessageImpl(
                                        message.partition(),
                                        message.bucket(),
                                        message.totalBuckets(),
                                        new DataIncrement(
                                                files,
                                                increment.deletedFiles(),
                                                increment.changelogFiles()),
                                        impl.compactIncrement())));
            }
            checkState(input.read() == -1, "Clustering output lost rows from its input.");
        }
        for (Committable committable : committables) {
            output.collect(new StreamRecord<>(committable));
        }
        emitted = true;
    }

    @Override
    public void close() throws Exception {
        try {
            if (rowIds != null) {
                rowIds.close();
            }
        } finally {
            try {
                if (write != null) {
                    write.close();
                }
            } finally {
                try {
                    if (memoryAllocator != null) {
                        memoryAllocator.release();
                    }
                    if (!emitted && mappingPaths != null) {
                        mappingPaths.forEach(table.fileIO()::deleteQuietly);
                    }
                    if (!emitted && preparedDataPaths != null) {
                        preparedDataPaths.forEach(table.fileIO()::deleteQuietly);
                    }
                    if (ioManager != null) {
                        ioManager.close();
                    }
                } finally {
                    super.close();
                }
            }
        }
    }
}
