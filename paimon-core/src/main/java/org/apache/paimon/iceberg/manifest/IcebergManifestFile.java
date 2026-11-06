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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
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

    /** How many entries {@link #decode} reads between two runs of its check. */
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
     * Decodes the whole file one entry at a time, drops each entry at once, and returns what the
     * file holds. Memory use does not depend on the size of the file.
     *
     * <p>A manifest list records the counters and the partition summaries of its manifests when it
     * is written, and a step that only adds files trusts them, so neither the length of a file nor
     * the entry of one list says anything about its contents. {@link ManifestFacts#disagreement}
     * compares the facts with an entry, and a file that is listed by more than one list is compared
     * with each of the entries.
     *
     * @param first the entry of a list that names the file, which the entries of the file inherit
     *     from when they leave a field unset
     * @param betweenEntries runs every {@link #VALIDATE_CHECK_INTERVAL} entries, and may throw to
     *     stop the scan. What it throws is not taken for damage.
     * @throws CorruptManifestException when the storage delivered the whole file and it does not
     *     decode
     * @throws IOException when the storage failed, or ended the read before the end of the file, so
     *     that a failing read is never reported as damage
     */
    public ManifestFacts decode(
            IcebergManifestFileMeta first, long fileSize, Runnable betweenEntries)
            throws IOException {
        Path path = pathFactory.toPath(new Path(first.manifestPath()).getName());
        ReadWatch watch = new ReadWatch(fileIO, fileSize);
        RuntimeException[] fromCheck = new RuntimeException[1];
        long[] counts = new long[6];
        boolean anyData = false;
        boolean anyDeletes = false;
        PartitionSummaryCollector partitionSummaries = new PartitionSummaryCollector();
        try (CloseableIterator<InternalRow> iterator =
                FileUtils.createFormatReader(watch, readerFactory, path, fileSize)
                        .toCloseableIterator()) {
            IcebergManifestEntrySerializer entrySerializer =
                    (IcebergManifestEntrySerializer) serializer;
            long entries = 0;
            while (iterator.hasNext()) {
                IcebergManifestEntry entry = entrySerializer.fromRow(iterator.next(), first);
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
                if (entry.file().content() == IcebergDataFileMeta.Content.DATA) {
                    anyData = true;
                } else {
                    anyDeletes = true;
                }
                partitionSummaries.collect(entry.file().partition());
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
            watch.throwIfStorageFailed(path, e);
            throw new CorruptManifestException(path + " cannot be decoded: " + e, e);
        }
        // A reader can take a read that ended early, at the end of a block, for the end of the
        // file, and then the facts differ. That is the storage failing, not damage.
        watch.throwIfStorageFailed(path, null);

        if (anyData && anyDeletes) {
            throw new CorruptManifestException(
                    path + " holds the entries of data files and of delete files");
        }
        return new ManifestFacts(
                fileSize, counts, anyData, anyDeletes, partitionSummaries.result(), first);
    }

    /**
     * What a manifest holds, in the terms of the manifest list entry that is written for it. It is
     * what the entries of the lists that name the manifest must agree with, whichever list reads
     * them: a reader skips a manifest by its counters and its partition summaries without looking
     * at it.
     */
    public static final class ManifestFacts {

        private final long length;
        private final long[] counts;
        private final boolean anyData;
        private final boolean anyDeletes;
        private final List<IcebergPartitionSummary> partitions;
        private final IcebergManifestFileMeta first;

        private ManifestFacts(
                long length,
                long[] counts,
                boolean anyData,
                boolean anyDeletes,
                List<IcebergPartitionSummary> partitions,
                IcebergManifestFileMeta first) {
            this.length = length;
            this.counts = counts;
            this.anyData = anyData;
            this.anyDeletes = anyDeletes;
            this.partitions = partitions;
            this.first = first;
        }

        /**
         * Why the entry of a manifest list cannot be of this manifest, or null when it can. The
         * fields that the file does not determine, such as the sequence numbers, are compared with
         * those of the entry that the file was decoded with, because a manifest that is carried
         * from list to list is listed the same way every time.
         */
        @Nullable
        public String disagreement(IcebergManifestFileMeta entry, long listedBy) {
            if (entry.manifestLength() != length) {
                return "its list records "
                        + entry.manifestLength()
                        + " bytes, the file has "
                        + length;
            }
            long[] listed =
                    new long[] {
                        entry.addedFilesCount(),
                        entry.existingFilesCount(),
                        entry.deletedFilesCount(),
                        entry.addedRowsCount(),
                        entry.existingRowsCount(),
                        entry.deletedRowsCount()
                    };
            if (!Arrays.equals(counts, listed)) {
                return "it holds (added, existing, deleted files and rows) "
                        + Arrays.toString(counts)
                        + ", and its list records "
                        + Arrays.toString(listed);
            }
            if ((anyData && entry.content() != Content.DATA)
                    || (anyDeletes && entry.content() != Content.DELETES)) {
                return "its list records the content "
                        + entry.content()
                        + ", which the file does not hold";
            }
            if (!partitions.equals(entry.partitions())) {
                return "its list records partition summaries that differ from those of its entries";
            }
            if (entry.partitionSpecId() != first.partitionSpecId()
                    || entry.sequenceNumber() != first.sequenceNumber()
                    || entry.minSequenceNumber() != first.minSequenceNumber()
                    || entry.addedSnapshotId() != first.addedSnapshotId()) {
                return "the lists that name it record its sequence numbers, snapshot or partition "
                        + "spec differently";
            }
            // what every manifest of Paimon is written with, so a list that names the manifest
            // once is held to it as well: there is one partition spec, and a manifest is
            // written for a snapshot at or before the one whose list names it
            if (entry.partitionSpecId() != IcebergPartitionSpec.SPEC_ID
                    || entry.sequenceNumber() != entry.addedSnapshotId()
                    || entry.sequenceNumber() > listedBy
                    || entry.minSequenceNumber() > entry.sequenceNumber()) {
                return "its partition spec, sequence numbers or snapshot do not fit the snapshot "
                        + listedBy
                        + " that lists it";
            }
            return null;
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

        private final PartitionSummaryCollector partitionSummaries =
                new PartitionSummaryCollector();
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

            partitionSummaries.collect(entry.file().partition());
        }

        @Override
        public IcebergManifestFileMeta result() throws IOException {
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
                    partitionSummaries.result(),
                    null);
        }
    }

    /**
     * Collects the partition summaries of a manifest list entry from the entries of its manifest.
     * The writer of a manifest and the check of one use it, so that they cannot differ.
     */
    private class PartitionSummaryCollector {

        private final SimpleStatsCollector collector;
        private final IcebergPartitionStatsCollector[] fieldCollectors;

        PartitionSummaryCollector() {
            this.fieldCollectors =
                    new IcebergPartitionStatsCollector[partitionType.getFieldCount()];
            SimpleColStatsCollector.Factory[] statsFactories =
                    new SimpleColStatsCollector.Factory[partitionType.getFieldCount()];
            for (int i = 0; i < partitionType.getFieldCount(); i++) {
                int position = i;
                statsFactories[i] =
                        () -> {
                            IcebergPartitionStatsCollector fieldCollector =
                                    new IcebergPartitionStatsCollector();
                            fieldCollectors[position] = fieldCollector;
                            return fieldCollector;
                        };
            }
            this.collector = new SimpleStatsCollector(partitionType, statsFactories);
        }

        void collect(InternalRow partition) {
            collector.collect(partition);
        }

        List<IcebergPartitionSummary> result() {
            SimpleColStats[] stats = collector.extract();
            List<IcebergPartitionSummary> summaries = new ArrayList<>();
            for (int i = 0; i < stats.length; i++) {
                SimpleColStats fieldStats = stats[i];
                DataType type = partitionType.getTypeAt(i);
                Object min = fieldStats.min();
                Object max = fieldStats.max();
                summaries.add(
                        new IcebergPartitionSummary(
                                Objects.requireNonNull(fieldStats.nullCount()) > 0,
                                fieldCollectors[i].containsNan(),
                                min == null ? null : toByteBuffer(type, min).array(),
                                max == null ? null : toByteBuffer(type, max).array()));
            }
            return summaries;
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
