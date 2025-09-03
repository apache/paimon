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

package org.apache.paimon.table.format;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.format.FormatTableAtomicCommitter.TempFileInfo;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A {@link BatchTableWrite} implementation for {@link FormatTable} that handles writing to format
 * tables (ORC, Parquet, CSV, JSON).
 *
 * @since 0.9.0
 */
@Public
public class FormatTableWrite implements BatchTableWrite {

    private final FormatTable formatTable;
    private final String commitUser;
    private final boolean overwrite;
    private final FileIO fileIO;
    private final RowType rowType;
    private final List<String> partitionKeys;
    private final FileFormat fileFormat;
    private final FormatWriterFactory writerFactory;

    private final Map<String, FormatWriter> partitionWriters;
    private final Map<String, Path> partitionPaths;
    private final Map<String, TempFileInfo> writtenFiles;
    private final FormatTableAtomicCommitter atomicCommitter;

    private RowType writeType;
    private IOManager ioManager;
    private MemorySegmentPool memoryPool;
    private MetricRegistry metricRegistry;

    public FormatTableWrite(FormatTable formatTable, String commitUser, boolean overwrite) {
        this.formatTable = formatTable;
        this.commitUser = commitUser;
        this.overwrite = overwrite;
        this.fileIO = formatTable.fileIO();
        this.rowType = formatTable.rowType();
        this.partitionKeys = formatTable.partitionKeys();
        this.writeType = rowType;

        // Initialize file format and writer factory
        this.fileFormat =
                FileFormat.fromIdentifier(
                        formatTable.format().name().toLowerCase(),
                        new Options(formatTable.options()));
        this.writerFactory = fileFormat.createWriterFactory(rowType);

        this.partitionWriters = new HashMap<>();
        this.partitionPaths = new HashMap<>();
        this.writtenFiles = new HashMap<>();
        this.atomicCommitter = FormatTableAtomicCommitter.create(formatTable);
    }

    @Override
    public FormatTableWrite withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    @Override
    public FormatTableWrite withWriteType(RowType writeType) {
        this.writeType = writeType;
        return this;
    }

    @Override
    public FormatTableWrite withMemoryPool(MemorySegmentPool memoryPool) {
        this.memoryPool = memoryPool;
        return this;
    }

    @Override
    public FormatTableWrite withMetricRegistry(MetricRegistry registry) {
        this.metricRegistry = registry;
        return this;
    }

    @Override
    public BinaryRow getPartition(InternalRow row) {
        if (partitionKeys.isEmpty()) {
            return new BinaryRow(0);
        }

        BinaryRow partition = new BinaryRow(partitionKeys.size());
        BinaryRowWriter writer = new BinaryRowWriter(partition);

        for (int i = 0; i < partitionKeys.size(); i++) {
            String partitionKey = partitionKeys.get(i);
            int fieldIndex = rowType.getFieldIndex(partitionKey);
            if (fieldIndex >= 0) {
                if (!row.isNullAt(fieldIndex)) {
                    switch (rowType.getTypeAt(fieldIndex).getTypeRoot()) {
                        case INTEGER:
                            writer.writeInt(i, row.getInt(fieldIndex));
                            break;
                        case VARCHAR:
                        case CHAR:
                            writer.writeString(i, row.getString(fieldIndex));
                            break;
                            // Add more types as needed
                        default:
                            writer.writeString(i, row.getString(fieldIndex));
                    }
                } else {
                    writer.setNullAt(i);
                }
            }
        }
        writer.complete();
        return partition;
    }

    @Override
    public int getBucket(InternalRow row) {
        // Format tables don't use bucketing
        return 0;
    }

    @Override
    public void write(InternalRow row) throws Exception {
        write(row, getBucket(row));
    }

    @Override
    public void write(InternalRow row, int bucket) throws Exception {
        BinaryRow partition = getPartition(row);
        String partitionKey = partitionToString(partition);

        FormatWriter writer = getOrCreateWriter(partitionKey, partition);
        writer.addElement(row);
    }

    @Override
    public void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle)
            throws Exception {
        // Convert bundle to individual rows and write them
        String partitionKey = partitionToString(partition);
        FormatWriter writer = getOrCreateWriter(partitionKey, partition);

        // Convert bundle to individual rows and write them
        // Note: BundleRecords doesn't have a records() method in the current API
        // This would need to be implemented based on the actual BundleRecords interface
        throw new UnsupportedOperationException(
                "Bundle writing not supported for format tables yet");
    }

    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        // Format tables don't support compaction in the traditional sense
        // This could be extended to merge files if needed
    }

    @Override
    public List<CommitMessage> prepareCommit() throws Exception {
        List<CommitMessage> commitMessages = new ArrayList<>();

        // Close all writers to finalize temporary files
        for (Map.Entry<String, FormatWriter> entry : partitionWriters.entrySet()) {
            String partitionKey = entry.getKey();
            FormatWriter writer = entry.getValue();

            // Close the writer to finalize the file
            writer.close();

            TempFileInfo tempFileInfo = writtenFiles.get(partitionKey);
            if (tempFileInfo != null) {
                commitMessages.add(new FormatTableCommitMessage(partitionKey, tempFileInfo));
            }
        }

        return commitMessages;
    }

    @Override
    public void close() throws Exception {
        // Close all writers
        for (FormatWriter writer : partitionWriters.values()) {
            if (writer != null) {
                try {
                    writer.close();
                } catch (Exception e) {
                    // Log but don't fail on close
                }
            }
        }
        partitionWriters.clear();
        partitionPaths.clear();
        writtenFiles.clear();
    }

    private FormatWriter getOrCreateWriter(String partitionKey, BinaryRow partition)
            throws IOException {
        FormatWriter writer = partitionWriters.get(partitionKey);
        if (writer == null) {
            // Create temporary file path using atomic committer
            Path tempPartitionPath = atomicCommitter.prepareTempLocation(partitionKey);
            String fileName = generateFileName();
            Path tempFilePath = new Path(tempPartitionPath, fileName);

            // Create final file path
            Path finalPartitionPath = createPartitionPath(partition);
            Path finalFilePath = new Path(finalPartitionPath, fileName);

            // Ensure temporary directory exists
            if (!fileIO.exists(tempPartitionPath)) {
                fileIO.mkdirs(tempPartitionPath);
            }

            PositionOutputStream outputStream = fileIO.newOutputStream(tempFilePath, false);
            writer = writerFactory.create(outputStream, getCompression());

            // Store temp file info for later commit
            TempFileInfo tempFileInfo = new TempFileInfo(tempFilePath, finalFilePath, partitionKey);

            partitionWriters.put(partitionKey, writer);
            partitionPaths.put(partitionKey, tempPartitionPath);
            writtenFiles.put(partitionKey, tempFileInfo);
        }
        return writer;
    }

    private Path createPartitionPath(BinaryRow partition) {
        Path basePath = new Path(formatTable.location());

        if (partitionKeys.isEmpty()) {
            return basePath;
        }

        StringBuilder pathBuilder = new StringBuilder();
        for (int i = 0; i < partitionKeys.size(); i++) {
            if (i > 0) {
                pathBuilder.append("/");
            }
            pathBuilder.append(partitionKeys.get(i)).append("=");
            // Convert partition field to string
            Object value = partition.isNullAt(i) ? "null" : partition.getString(i);
            pathBuilder.append(value);
        }

        return new Path(basePath, pathBuilder.toString());
    }

    private String partitionToString(BinaryRow partition) {
        if (partition.getFieldCount() == 0) {
            return "default";
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partition.getFieldCount(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(partition.isNullAt(i) ? "null" : partition.getString(i));
        }
        return sb.toString();
    }

    private String generateFileName() {
        String timestamp =
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        String uuid = UUID.randomUUID().toString().substring(0, 8);
        String extension = getFileExtension();
        return String.format("data-%s-%s-%s.%s", commitUser, timestamp, uuid, extension);
    }

    private String getFileExtension() {
        switch (formatTable.format()) {
            case PARQUET:
                return "parquet";
            case ORC:
                return "orc";
            case CSV:
                return "csv";
            case JSON:
                return "json";
            default:
                return "data";
        }
    }

    private String getCompression() {
        return formatTable.options().getOrDefault("compression", "snappy");
    }

    /** Commit message for format table writes. */
    public static class FormatTableCommitMessage implements CommitMessage {
        private final String partitionString;
        private final TempFileInfo tempFileInfo;

        public FormatTableCommitMessage(String partition, TempFileInfo tempFileInfo) {
            this.partitionString = partition;
            this.tempFileInfo = tempFileInfo;
        }

        public String getPartition() {
            return partitionString;
        }

        public TempFileInfo getTempFileInfo() {
            return tempFileInfo;
        }

        public String getFilePath() {
            return tempFileInfo.getFinalPath().toString();
        }

        @Override
        public BinaryRow partition() {
            // For format tables, we can return an empty partition for non-partitioned tables
            // or parse the partition string back to BinaryRow for partitioned tables
            return new BinaryRow(0);
        }

        @Override
        public int bucket() {
            // Format tables don't use bucketing
            return 0;
        }

        @Override
        public Integer totalBuckets() {
            // Format tables don't use bucketing
            return 1;
        }

        @Override
        public String toString() {
            return String.format(
                    "FormatTableCommitMessage{partition='%s', tempFileInfo=%s}",
                    partitionString, tempFileInfo);
        }
    }
}
