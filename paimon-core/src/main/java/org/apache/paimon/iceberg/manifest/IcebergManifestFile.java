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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.iceberg.IcebergPathFactory;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta.Content;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergPartitionSpec;
import org.apache.paimon.io.RollingFileWriterImpl;
import org.apache.paimon.io.SingleFileWriter;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.statistics.FullSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.PathFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.iceberg.manifest.IcebergConversions.toByteBuffer;

/**
 * This file includes several Iceberg {@link ManifestEntry}s, representing the additional changes
 * since last snapshot.
 */
public class IcebergManifestFile extends ObjectsFile<IcebergManifestEntry> {

    private static final long UNASSIGNED_SEQ = -1L;

    /** How many entries {@link #validate} decodes between two runs of its check. */
    public static final int VALIDATE_CHECK_INTERVAL = 1024;

    private final RowType partitionType;
    private final FormatReaderFactory readerFactory;
    private final FormatWriterFactory writerFactory;
    private final MemorySize targetFileSize;

    public IcebergManifestFile(
            FileIO fileIO,
            RowType partitionType,
            boolean withFirstRowId,
            FormatReaderFactory readerFactory,
            FormatWriterFactory writerFactory,
            String compression,
            PathFactory pathFactory,
            MemorySize targetFileSize) {
        super(
                fileIO,
                new IcebergManifestEntrySerializer(partitionType, withFirstRowId),
                IcebergManifestEntry.schema(partitionType, withFirstRowId),
                readerFactory,
                writerFactory,
                compression,
                pathFactory,
                null);
        this.partitionType = partitionType;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.targetFileSize = targetFileSize;
    }

    @VisibleForTesting
    public String compression() {
        return compression;
    }

    public static IcebergManifestFile create(FileStoreTable table, IcebergPathFactory pathFactory) {
        RowType partitionType = table.schema().logicalPartitionType();
        Options avroOptions = Options.fromMap(table.options());
        boolean withFirstRowId =
                avroOptions.get(IcebergOptions.FORMAT_VERSION) >= IcebergMetadata.FORMAT_VERSION_V3;
        RowType entryType = IcebergManifestEntry.schema(partitionType, withFirstRowId);
        // https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/ManifestReader.java
        avroOptions.set(
                "avro.row-name-mapping",
                "org.apache.paimon.avro.generated.record:manifest_entry,"
                        + "iceberg:true,"
                        + "manifest_entry_data_file:r2,"
                        + "r2_partition:r102,"
                        + "kv_name_r2_null_value_counts:k121_v122,"
                        + "k_id_k121_v122:121,"
                        + "v_id_k121_v122:122,"
                        + "kv_name_r2_lower_bounds:k126_v127,"
                        + "k_id_k126_v127:126,"
                        + "v_id_k126_v127:127,"
                        + "kv_name_r2_upper_bounds:k129_v130,"
                        + "k_id_k129_v130:129,"
                        + "v_id_k129_v130:130");
        FileFormat manifestFileAvro = FileFormat.manifestFormat(new CoreOptions(avroOptions));
        return new IcebergManifestFile(
                table.fileIO(),
                partitionType,
                withFirstRowId,
                manifestFileAvro.createReaderFactory(entryType, entryType, new ArrayList<>()),
                manifestFileAvro.createWriterFactory(entryType),
                avroOptions.get(IcebergOptions.MANIFEST_COMPRESSION),
                pathFactory.manifestFileFactory(),
                table.coreOptions().manifestTargetSize());
    }

    public List<IcebergManifestEntry> read(IcebergManifestFileMeta meta) {
        return read(meta, null);
    }

    public List<IcebergManifestEntry> read(IcebergManifestFileMeta meta, @Nullable Long fileSize) {
        String fileName = new Path(meta.manifestPath()).getName();
        try {
            Path path = pathFactory.toPath(fileName);

            return readFromIterator(
                    meta,
                    createIterator(path, fileSize),
                    (IcebergManifestEntrySerializer) serializer,
                    Filter.alwaysTrue());
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + fileName, e);
        }
    }

    private static List<IcebergManifestEntry> readFromIterator(
            IcebergManifestFileMeta meta,
            CloseableIterator<InternalRow> inputIterator,
            IcebergManifestEntrySerializer serializer,
            Filter<InternalRow> readFilter) {
        try (CloseableIterator<InternalRow> iterator = inputIterator) {
            List<IcebergManifestEntry> result = new ArrayList<>();
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                if (readFilter.test(row)) {
                    result.add(serializer.fromRow(row, meta));
                }
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decodes the whole file one entry at a time, drops each entry at once, and compares the number
     * of files and rows that it found with the counters of its manifest list entry. Memory use does
     * not depend on the size of the file.
     *
     * <p>A manifest list records the counters when it is written, and a step that only adds files
     * trusts them, so the length of a file says nothing about its contents.
     *
     * @param betweenEntries runs every {@link #VALIDATE_CHECK_INTERVAL} entries, and may throw to
     *     stop the scan. What it throws is not taken for damage.
     * @throws CorruptManifestException when the storage delivered the whole file and it does not
     *     decode, or when the counts differ
     * @throws IOException when the storage failed, or ended the read before the end of the file, so
     *     that a failing read is never reported as damage
     */
    public void validate(IcebergManifestFileMeta meta, long fileSize, Runnable betweenEntries)
            throws IOException {
        Path path = pathFactory.toPath(new Path(meta.manifestPath()).getName());
        ReadWatch watch = new ReadWatch(fileIO, fileSize);
        RuntimeException[] fromCheck = new RuntimeException[1];
        long[] counts = new long[6];
        try (CloseableIterator<InternalRow> iterator =
                FileUtils.createFormatReader(watch, readerFactory, path, fileSize)
                        .toCloseableIterator()) {
            IcebergManifestEntrySerializer entrySerializer =
                    (IcebergManifestEntrySerializer) serializer;
            long entries = 0;
            while (iterator.hasNext()) {
                IcebergManifestEntry entry = entrySerializer.fromRow(iterator.next(), meta);
                long rows = entry.file().recordCount();
                switch (entry.status()) {
                    case ADDED:
                        counts[0]++;
                        counts[3] += rows;
                        break;
                    case EXISTING:
                        counts[1]++;
                        counts[4] += rows;
                        break;
                    case DELETED:
                        counts[2]++;
                        counts[5] += rows;
                        break;
                    default:
                        throw new IllegalStateException("Unknown status " + entry.status());
                }
                if (++entries % VALIDATE_CHECK_INTERVAL == 0) {
                    try {
                        betweenEntries.run();
                    } catch (RuntimeException e) {
                        fromCheck[0] = e;
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            if (e == fromCheck[0]) {
                throw (RuntimeException) e;
            }
            if (watch.failure != null) {
                throw watch.failure;
            }
            if (watch.endedShort) {
                throw new IOException("A read of " + path + " ended before the end of the file", e);
            }
            throw new CorruptManifestException(path + " cannot be decoded: " + e, e);
        }

        long[] expected =
                new long[] {
                    meta.addedFilesCount(),
                    meta.existingFilesCount(),
                    meta.deletedFilesCount(),
                    meta.addedRowsCount(),
                    meta.existingRowsCount(),
                    meta.deletedRowsCount()
                };
        if (!Arrays.equals(counts, expected)) {
            throw new CorruptManifestException(
                    path
                            + " holds (added, existing, deleted files and rows) "
                            + Arrays.toString(counts)
                            + ", and its manifest list records "
                            + Arrays.toString(expected));
        }
    }

    /** The bytes of a manifest were delivered by the storage, and they are not a valid manifest. */
    public static class CorruptManifestException extends RuntimeException {

        public CorruptManifestException(String message) {
            super(message);
        }

        public CorruptManifestException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Passes every call on to the file system and notes whether a read failed or ended early, so
     * that a decoder error can be told from a storage error.
     */
    private static class ReadWatch implements FileIO {

        private final FileIO delegate;
        private final long fileSize;
        private volatile IOException failure;
        private volatile boolean endedShort;

        private ReadWatch(FileIO delegate, long fileSize) {
            this.delegate = delegate;
            this.fileSize = fileSize;
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            SeekableInputStream in;
            try {
                in = delegate.newInputStream(path);
            } catch (IOException e) {
                failure = e;
                throw e;
            }
            return new SeekableInputStream() {
                @Override
                public void seek(long desired) throws IOException {
                    try {
                        in.seek(desired);
                    } catch (IOException e) {
                        failure = e;
                        throw e;
                    }
                }

                @Override
                public long getPos() throws IOException {
                    return in.getPos();
                }

                @Override
                public int read() throws IOException {
                    try {
                        int b = in.read();
                        if (b < 0 && in.getPos() < fileSize) {
                            endedShort = true;
                        }
                        return b;
                    } catch (IOException e) {
                        failure = e;
                        throw e;
                    }
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    try {
                        int n = in.read(b, off, len);
                        if (n < 0 && in.getPos() < fileSize) {
                            endedShort = true;
                        }
                        return n;
                    } catch (IOException e) {
                        failure = e;
                        throw e;
                    }
                }

                @Override
                public void close() throws IOException {
                    in.close();
                }
            };
        }

        @Override
        public boolean isObjectStore() {
            return delegate.isObjectStore();
        }

        @Override
        public void configure(CatalogContext context) {
            delegate.configure(context);
        }

        @Override
        public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                throws IOException {
            return delegate.newOutputStream(path, overwrite);
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            return delegate.getFileStatus(path);
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            return delegate.listStatus(path);
        }

        @Override
        public boolean exists(Path path) throws IOException {
            return delegate.exists(path);
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            return delegate.delete(path, recursive);
        }

        @Override
        public boolean mkdirs(Path path) throws IOException {
            return delegate.mkdirs(path);
        }

        @Override
        public boolean rename(Path src, Path dst) throws IOException {
            return delegate.rename(src, dst);
        }
    }

    public List<IcebergManifestFileMeta> rollingWrite(
            Iterator<IcebergManifestEntry> entries, long sequenceNumber) {

        return rollingWrite(entries, sequenceNumber, Content.DATA);
    }

    public List<IcebergManifestFileMeta> rollingWrite(
            Iterator<IcebergManifestEntry> entries, long sequenceNumber, Content content) {
        RollingFileWriterImpl<IcebergManifestEntry, IcebergManifestFileMeta> writer =
                new RollingFileWriterImpl<>(
                        () -> createWriter(sequenceNumber, content),
                        targetFileSize.getBytes(),
                        Long.MAX_VALUE);
        try {
            writer.write(entries);
            writer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return writer.result();
    }

    public SingleFileWriter<IcebergManifestEntry, IcebergManifestFileMeta> createWriter(
            long sequenceNumber, Content content) {
        return new IcebergManifestEntryWriter(
                writerFactory, pathFactory.newPath(), compression, sequenceNumber, content);
    }

    private class IcebergManifestEntryWriter
            extends SingleFileWriter<IcebergManifestEntry, IcebergManifestFileMeta> {

        private final SimpleStatsCollector partitionStatsCollector;
        private final IcebergPartitionStatsCollector[] partitionStatsCollectors;
        private final long sequenceNumber;

        private int addedFilesCount = 0;
        private int existingFilesCount = 0;
        private int deletedFilesCount = 0;
        private long addedRowsCount = 0;
        private long existingRowsCount = 0;
        private long deletedRowsCount = 0;
        private Long minSequenceNumber = null;

        private final Content content;

        IcebergManifestEntryWriter(
                FormatWriterFactory factory,
                Path path,
                String fileCompression,
                long sequenceNumber,
                Content content) {
            super(
                    IcebergManifestFile.this.fileIO,
                    factory,
                    path,
                    serializer::toRow,
                    fileCompression,
                    false);
            this.partitionStatsCollectors =
                    new IcebergPartitionStatsCollector[partitionType.getFieldCount()];
            SimpleColStatsCollector.Factory[] statsFactories =
                    new SimpleColStatsCollector.Factory[partitionType.getFieldCount()];
            for (int i = 0; i < partitionType.getFieldCount(); i++) {
                int position = i;
                statsFactories[i] =
                        () -> {
                            IcebergPartitionStatsCollector collector =
                                    new IcebergPartitionStatsCollector();
                            partitionStatsCollectors[position] = collector;
                            return collector;
                        };
            }
            this.partitionStatsCollector = new SimpleStatsCollector(partitionType, statsFactories);
            this.sequenceNumber = sequenceNumber;
            this.content = content;
        }

        @Override
        public void write(IcebergManifestEntry entry) throws IOException {
            super.write(entry);

            switch (entry.status()) {
                case ADDED:
                    addedFilesCount += 1;
                    addedRowsCount += entry.file().recordCount();
                    break;
                case EXISTING:
                    existingFilesCount += 1;
                    existingRowsCount += entry.file().recordCount();
                    break;
                case DELETED:
                    deletedFilesCount += 1;
                    deletedRowsCount += entry.file().recordCount();
                    break;
            }

            if (minSequenceNumber == null || minSequenceNumber > entry.sequenceNumber()) {
                minSequenceNumber = entry.sequenceNumber();
            }

            partitionStatsCollector.collect(entry.file().partition());
        }

        @Override
        public IcebergManifestFileMeta result() throws IOException {
            SimpleColStats[] stats = partitionStatsCollector.extract();
            List<IcebergPartitionSummary> partitionSummaries = new ArrayList<>();
            for (int i = 0; i < stats.length; i++) {
                SimpleColStats fieldStats = stats[i];
                DataType type = partitionType.getTypeAt(i);
                Object min = fieldStats.min();
                Object max = fieldStats.max();
                partitionSummaries.add(
                        new IcebergPartitionSummary(
                                Objects.requireNonNull(fieldStats.nullCount()) > 0,
                                partitionStatsCollectors[i].containsNan(),
                                min == null ? null : toByteBuffer(type, min).array(),
                                max == null ? null : toByteBuffer(type, max).array()));
            }
            return new IcebergManifestFileMeta(
                    path.toString(),
                    outputBytes(),
                    IcebergPartitionSpec.SPEC_ID,
                    content,
                    sequenceNumber,
                    minSequenceNumber != null ? minSequenceNumber : UNASSIGNED_SEQ,
                    sequenceNumber,
                    addedFilesCount,
                    existingFilesCount,
                    deletedFilesCount,
                    addedRowsCount,
                    existingRowsCount,
                    deletedRowsCount,
                    partitionSummaries,
                    null);
        }
    }

    private static class IcebergPartitionStatsCollector extends FullSimpleColStatsCollector {

        private boolean containsNan;

        @Override
        public void collect(Object field, Serializer<Object> fieldSerializer) {
            if ((field instanceof Float && Float.isNaN((Float) field))
                    || (field instanceof Double && Double.isNaN((Double) field))) {
                containsNan = true;
                return;
            }
            super.collect(field, fieldSerializer);
        }

        private boolean containsNan() {
            return containsNan;
        }
    }
}
