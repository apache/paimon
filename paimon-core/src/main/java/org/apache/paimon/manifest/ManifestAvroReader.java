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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.avro.AvroBlockReader;
import org.apache.paimon.format.avro.AvroFileFormat;
import org.apache.paimon.format.avro.AvroRawBlock;
import org.apache.paimon.format.avro.AvroRecordDecoder;
import org.apache.paimon.format.avro.AvroRecordDecoder.FieldDecoder;
import org.apache.paimon.format.avro.AvroRecordDecoder.FieldType;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/** Reader which exposes reusable raw blocks from a Manifest Avro file. */
public final class ManifestAvroReader implements AutoCloseable {

    private static final String[] TOP_LEVEL_FIELDS = {
        ManifestSchemaUtils.FORMAT_IDENTIFIER_FIELD,
        ManifestEntry.KIND,
        ManifestEntry.PARTITION,
        ManifestEntry.BUCKET,
        ManifestEntry.TOTAL_BUCKETS,
        ManifestEntry.FILE
    };

    private final AvroBlockReader stream;
    private final DecoderContext decoderContext;
    private final boolean rawBlockCopySupported;
    private final ReadStatistics readStatistics = new ReadStatistics();

    private long blockOrdinal = -1;

    ManifestAvroReader(InputStream input, AvroFileFormat avroFileFormat) throws IOException {
        AvroBlockReader stream = null;
        try {
            stream = new AvroBlockReader(input);
            this.stream = stream;
            this.decoderContext = new DecoderContext(stream.createRecordDecoder());
            this.rawBlockCopySupported =
                    stream.supportsRawBlockCopy(avroFileFormat, ManifestEntry.MANIFEST_ROW_TYPE);
        } catch (IOException | RuntimeException | Error failure) {
            IOUtils.closeQuietly(stream == null ? input : stream);
            throw failure;
        }
    }

    /** Returns whether another raw Avro block is available. */
    public boolean hasNext() throws IOException {
        return stream.hasNextBlock();
    }

    /** Returns the next raw block without decompressing it. */
    public RawBlock next() throws IOException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return new RawBlock(
                decoderContext,
                rawBlockCopySupported,
                stream.nextBorrowedRawBlock(),
                ++blockOrdinal);
    }

    /**
     * Returns an iterator over projected rows from all remaining blocks.
     *
     * <p>The returned row is reused within each block and must be consumed before the iterator
     * advances. Closing the iterator closes this reader.
     */
    public CloseableIterator<InternalRow> read(
            RowType projectedType,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable BucketFilter bucketFilter) {
        return new RowsIterator(projectedType, partitionFilter, bucketFilter);
    }

    long decodedDataFiles() {
        return readStatistics.decodedDataFiles;
    }

    long skippedDataFiles() {
        return readStatistics.skippedDataFiles;
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }

    /** Borrowed raw block which must be consumed before the enclosing reader advances. */
    public static final class RawBlock {

        private final DecoderContext decoderContext;
        private final boolean rawBlockCopySupported;
        private final AvroRawBlock block;
        private final long blockOrdinal;
        private final long blockRecordCount;

        private RawBlock(
                DecoderContext decoderContext,
                boolean rawBlockCopySupported,
                AvroRawBlock block,
                long blockOrdinal) {
            this.decoderContext = decoderContext;
            this.rawBlockCopySupported = rawBlockCopySupported;
            this.block = block;
            this.blockOrdinal = blockOrdinal;
            this.blockRecordCount = block.recordCount();
        }

        /** Lazily decompresses this block and returns an iterator over one reusable row. */
        public RowIterator toRows(RowType projectRowType) throws IOException {
            return toRows(projectRowType, null, null, null);
        }

        private RowIterator toRows(
                RowType projectRowType,
                @Nullable PartitionPredicate partitionFilter,
                @Nullable BucketFilter bucketFilter,
                @Nullable ReadStatistics readStatistics)
                throws IOException {
            ManifestEntryDecoder recordDecoder = decoderContext.recordDecoder(projectRowType);

            ByteBuffer decompressed = decoderContext.decompress(block);
            decoderContext.decoder.reset(decompressed);
            GenericRow row = recordDecoder.createRow();
            return new RowIterator(
                    blockRecordCount,
                    decoderContext.decoder,
                    recordDecoder,
                    row,
                    partitionFilter,
                    bucketFilter,
                    readStatistics);
        }

        public long blockOrdinal() {
            return blockOrdinal;
        }

        public long recordCount() {
            return blockRecordCount;
        }

        public boolean rawBlockCopySupported() {
            return rawBlockCopySupported;
        }

        public AvroRawBlock encodedBlock() {
            return block;
        }
    }

    private final class RowsIterator implements CloseableIterator<InternalRow> {

        private final RowType projectedType;
        private final @Nullable PartitionPredicate partitionFilter;
        private final @Nullable BucketFilter bucketFilter;

        private @Nullable RowIterator rows;
        private boolean closed;

        private RowsIterator(
                RowType projectedType,
                @Nullable PartitionPredicate partitionFilter,
                @Nullable BucketFilter bucketFilter) {
            this.projectedType = projectedType;
            this.partitionFilter = partitionFilter;
            this.bucketFilter = bucketFilter;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            try {
                while (rows == null || !rows.hasNext()) {
                    if (!ManifestAvroReader.this.hasNext()) {
                        return false;
                    }
                    rows =
                            ManifestAvroReader.this
                                    .next()
                                    .toRows(
                                            projectedType,
                                            partitionFilter,
                                            bucketFilter,
                                            readStatistics);
                }
                return true;
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to decode Manifest Avro block.", e);
            }
        }

        @Override
        public InternalRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return rows.next();
        }

        @Override
        public void close() throws IOException {
            closed = true;
            ManifestAvroReader.this.close();
        }
    }

    /** Decoder state shared by the borrowed blocks produced by one reader. */
    private static final class DecoderContext {

        private final AvroRecordDecoder decoder;

        private @Nullable ByteBuffer decompressionBuffer;
        private RowType projectedRowType;
        private ManifestEntryDecoder recordDecoder;

        private DecoderContext(AvroRecordDecoder decoder) {
            this.decoder = decoder;
        }

        private ByteBuffer decompress(AvroRawBlock block) throws IOException {
            decompressionBuffer = block.decompress(decompressionBuffer);
            return decompressionBuffer;
        }

        private ManifestEntryDecoder recordDecoder(RowType rowType) {
            if (!rowType.equals(projectedRowType)) {
                recordDecoder = new ManifestEntryDecoder(decoder, rowType);
                projectedRowType = rowType;
            }
            return recordDecoder;
        }
    }

    /** Iterator over the reusable row decoded from one borrowed block. */
    public static final class RowIterator implements Iterator<GenericRow> {

        private final AvroRecordDecoder decoder;
        private final ManifestEntryDecoder recordDecoder;
        private final GenericRow row;
        private final @Nullable PartitionPredicate partitionFilter;
        private final @Nullable BucketFilter bucketFilter;
        private final @Nullable ReadStatistics readStatistics;
        private final boolean filtered;

        private long blockRemaining;
        private long blockRecordIndex = -1;
        private boolean nextReady;
        private @Nullable ByteBuffer encodedRecord;

        private RowIterator(
                long recordCount,
                AvroRecordDecoder decoder,
                ManifestEntryDecoder recordDecoder,
                GenericRow row,
                @Nullable PartitionPredicate partitionFilter,
                @Nullable BucketFilter bucketFilter,
                @Nullable ReadStatistics readStatistics) {
            blockRemaining = recordCount;
            this.decoder = decoder;
            this.recordDecoder = recordDecoder;
            this.row = row;
            this.partitionFilter = partitionFilter;
            this.bucketFilter = bucketFilter;
            this.readStatistics = readStatistics;
            this.filtered = partitionFilter != null || bucketFilter != null;
        }

        @Override
        public boolean hasNext() {
            try {
                if (!filtered) {
                    ensureBlockFullyConsumed();
                    return blockRemaining > 0;
                }
                if (nextReady) {
                    return true;
                }
                while (blockRemaining > 0) {
                    blockRecordIndex++;
                    int recordStart = decoder.absolutePosition();
                    boolean selected =
                            recordDecoder.read(decoder, row, partitionFilter, bucketFilter);
                    recordRead(selected);
                    blockRemaining--;
                    if (selected) {
                        encodedRecord =
                                decoder.borrowedView(recordStart, decoder.absolutePosition());
                        nextReady = true;
                        return true;
                    }
                }
                ensureBlockFullyConsumed();
                return false;
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Failed to decode projected Manifest Avro record.", e);
            }
        }

        @Override
        public GenericRow next() {
            if (filtered) {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                nextReady = false;
                return row;
            }
            try {
                if (blockRemaining == 0) {
                    throw new NoSuchElementException();
                }
                blockRecordIndex++;
                int recordStart = decoder.absolutePosition();
                recordDecoder.read(decoder, row, null, null);
                encodedRecord = decoder.borrowedView(recordStart, decoder.absolutePosition());
                recordRead(true);
                blockRemaining--;
                return row;
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Failed to decode projected Manifest Avro record.", e);
            }
        }

        public long recordIndex() {
            checkCurrentRecord();
            return blockRecordIndex;
        }

        /** Returns a borrowed encoded view of the current complete Avro record. */
        public ByteBuffer encodedRecord() {
            checkCurrentRecord();
            if (encodedRecord == null) {
                throw new IllegalStateException("No current Manifest Avro record.");
            }
            return encodedRecord;
        }

        private void recordRead(boolean selected) {
            if (readStatistics != null) {
                if (selected && recordDecoder.decodesDataFile()) {
                    readStatistics.decodedDataFiles++;
                } else {
                    readStatistics.skippedDataFiles++;
                }
            }
        }

        private void ensureBlockFullyConsumed() throws IOException {
            if (blockRemaining == 0 && !decoder.isEnd()) {
                throw new IOException("Manifest Avro block contains trailing undecoded bytes.");
            }
        }

        private void checkCurrentRecord() {
            if (blockRecordIndex < 0) {
                throw new IllegalStateException("No current Manifest Avro record.");
            }
        }
    }

    private static final class ReadStatistics {

        private long decodedDataFiles;
        private long skippedDataFiles;
    }

    private static final class ManifestEntryDecoder {

        private final int projectedFieldCount;
        private final int versionPosition;
        private final int kindPosition;
        private final int partitionPosition;
        private final int bucketPosition;
        private final int totalBucketsPosition;
        private final int filePosition;
        private final FieldDecoder fileReader;

        private ManifestEntryDecoder(AvroRecordDecoder decoder, RowType projectedType) {
            validateTopLevelSchema(decoder);
            this.projectedFieldCount = projectedType.getFieldCount();
            this.filePosition = projectedType.getFieldIndex(ManifestEntry.FILE);
            this.versionPosition =
                    projectedType.getFieldIndex(ManifestSchemaUtils.FORMAT_IDENTIFIER_FIELD);
            this.kindPosition = projectedType.getFieldIndex(ManifestEntry.KIND);
            this.partitionPosition = projectedType.getFieldIndex(ManifestEntry.PARTITION);
            this.bucketPosition = projectedType.getFieldIndex(ManifestEntry.BUCKET);
            this.totalBucketsPosition = projectedType.getFieldIndex(ManifestEntry.TOTAL_BUCKETS);

            this.fileReader =
                    decoder.createFieldDecoder(
                            5, filePosition < 0 ? null : projectedType.getTypeAt(filePosition));
        }

        private GenericRow createRow() {
            return new GenericRow(projectedFieldCount);
        }

        private boolean read(
                AvroRecordDecoder decoder,
                GenericRow row,
                @Nullable PartitionPredicate partitionFilter,
                @Nullable BucketFilter bucketFilter)
                throws IOException {
            if (!decoder.readRecordStart()) {
                throw new IOException("Unexpected null or non-record Manifest Avro value.");
            }

            int version = decoder.readInt();
            ManifestEntrySerializer.checkFormatIdentifier(version);
            if (versionPosition >= 0) {
                row.setField(versionPosition, version);
            }

            int kind = decoder.readInt();
            if (kindPosition >= 0) {
                row.setField(kindPosition, (byte) kind);
            }

            boolean partitionNeededForFilter = partitionFilter != null || bucketFilter != null;
            byte[] partitionBytes;
            if (partitionPosition < 0 && !partitionNeededForFilter) {
                decoder.skipBytes();
                partitionBytes = null;
            } else {
                partitionBytes = decoder.readBytes();
                if (partitionPosition >= 0) {
                    row.setField(partitionPosition, partitionBytes);
                }
            }

            BinaryRow partition =
                    partitionNeededForFilter ? deserializeBinaryRow(partitionBytes) : null;
            if (partitionNeededForFilter) {
                if (partitionFilter != null && !partitionFilter.test(partition)) {
                    decoder.readInt();
                    decoder.readInt();
                    fileReader.skip(decoder);
                    return false;
                }
            }

            int bucket = decoder.readInt();
            if (bucketPosition >= 0) {
                row.setField(bucketPosition, bucket);
            }

            int totalBuckets = decoder.readInt();
            if (totalBucketsPosition >= 0) {
                row.setField(totalBucketsPosition, totalBuckets);
            }

            if (bucketFilter != null && !bucketFilter.test(partition, bucket, totalBuckets)) {
                fileReader.skip(decoder);
                return false;
            }

            if (filePosition < 0) {
                fileReader.skip(decoder);
            } else {
                row.setField(filePosition, fileReader.read(decoder, row.getField(filePosition)));
            }
            return true;
        }

        private boolean decodesDataFile() {
            return filePosition >= 0;
        }

        private static void validateTopLevelSchema(AvroRecordDecoder decoder) {
            if (decoder.fieldCount() != TOP_LEVEL_FIELDS.length) {
                throw new IllegalArgumentException(
                        String.format(
                                "Manifest Avro schema has %s top-level fields, expected %s.",
                                decoder.fieldCount(), TOP_LEVEL_FIELDS.length));
            }

            FieldType[] expectedTypes = {
                FieldType.INT,
                FieldType.INT,
                FieldType.BYTES,
                FieldType.INT,
                FieldType.INT,
                FieldType.RECORD
            };
            for (int i = 0; i < TOP_LEVEL_FIELDS.length; i++) {
                String actualName = decoder.fieldName(i);
                String expectedName = TOP_LEVEL_FIELDS[i];
                if (!expectedName.equals(actualName)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Unexpected Manifest Avro field at position %s: expected %s but found %s.",
                                    i, expectedName, actualName));
                }
                FieldType actualType = decoder.fieldType(i);
                if (actualType != expectedTypes[i]) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Unexpected Manifest Avro type for field %s: expected %s but found %s.",
                                    actualName, expectedTypes[i], actualType));
                }
            }
        }
    }
}
