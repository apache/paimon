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

/**
 * Schema-aware Avro reader which projects fields and filters before decoding data file metadata.
 *
 * <p>This reader also exposes reusable raw Avro blocks.
 */
public final class ManifestAvroReader implements AutoCloseable {

    private final AvroBlockReader blockReader;
    private final DecoderContext decoderContext;
    private final boolean rawBlockCopySupported;

    private long blockOrdinal = -1;

    ManifestAvroReader(InputStream input) throws IOException {
        AvroBlockReader blockReader = null;
        try {
            blockReader = new AvroBlockReader(input);
            this.blockReader = blockReader;
            this.decoderContext = new DecoderContext(blockReader.createRecordDecoder());
            this.rawBlockCopySupported =
                    blockReader.supportsRawBlockCopy(ManifestEntry.MANIFEST_ROW_TYPE);
        } catch (IOException | RuntimeException | Error failure) {
            IOUtils.closeQuietly(blockReader == null ? input : blockReader);
            throw failure;
        }
    }

    /** Returns whether another raw Avro block is available. */
    public boolean hasNext() throws IOException {
        return blockReader.hasNextBlock();
    }

    /** Returns the next raw block without decompressing it. */
    public RawBlock next() throws IOException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return new RawBlock(
                decoderContext,
                rawBlockCopySupported,
                blockReader.nextBorrowedRawBlock(),
                ++blockOrdinal);
    }

    /**
     * Returns an iterator over projected rows from all remaining blocks.
     *
     * <p>Every returned row has independent backing data. Closing the iterator closes this reader.
     */
    public CloseableIterator<InternalRow> read(
            RowType projectedType,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable BucketFilter bucketFilter) {
        return new CloseableIterator<InternalRow>() {

            private @Nullable RowIterator rows;
            private boolean closed;

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
                                                false);
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
        };
    }

    @Override
    public void close() throws IOException {
        blockReader.close();
    }

    private static class ManifestRecordDecoder {

        private static final String[] TOP_LEVEL_FIELDS = {
            ManifestSchemaUtils.FORMAT_IDENTIFIER_FIELD,
            ManifestEntry.KIND,
            ManifestEntry.PARTITION,
            ManifestEntry.BUCKET,
            ManifestEntry.TOTAL_BUCKETS,
            ManifestEntry.FILE
        };

        private final int projectedFieldCount;
        private final int versionPosition;
        private final int kindPosition;
        private final int partitionPosition;
        private final int bucketPosition;
        private final int totalBucketsPosition;
        private final int filePosition;
        private final FieldDecoder fileReader;

        private ManifestRecordDecoder(AvroRecordDecoder decoder, RowType projectedType) {
            this.projectedFieldCount = projectedType.getFieldCount();
            this.versionPosition =
                    projectedType.getFieldIndex(ManifestSchemaUtils.FORMAT_IDENTIFIER_FIELD);
            this.kindPosition = projectedType.getFieldIndex(ManifestEntry.KIND);
            this.partitionPosition = projectedType.getFieldIndex(ManifestEntry.PARTITION);
            this.bucketPosition = projectedType.getFieldIndex(ManifestEntry.BUCKET);
            this.totalBucketsPosition = projectedType.getFieldIndex(ManifestEntry.TOTAL_BUCKETS);
            this.filePosition = projectedType.getFieldIndex(ManifestEntry.FILE);

            validateTopLevelFields(decoder);
            // Manifest v2 has a fixed top-level layout, but DataFileMeta has gained nullable
            // fields. Build this reader from the writer schema so legacy files with fewer nested
            // fields still decode and expose the missing projected fields as null.
            fileReader =
                    decoder.createFieldDecoder(
                            5, filePosition >= 0 ? projectedType.getTypeAt(filePosition) : null);
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
            int kind = decoder.readInt();
            boolean partitionNeededForFilter = partitionFilter != null || bucketFilter != null;
            byte[] partitionBytes;
            if (partitionPosition >= 0 || partitionNeededForFilter) {
                partitionBytes = decoder.readBytes();
            } else {
                decoder.skipBytes();
                partitionBytes = null;
            }

            BinaryRow partition =
                    partitionNeededForFilter ? deserializeBinaryRow(partitionBytes) : null;
            if (partitionFilter != null && !partitionFilter.test(partition)) {
                skipBucketAndFile(decoder);
                return false;
            }

            boolean bucketNeededForFilter = bucketFilter != null;
            int bucket;
            if (bucketPosition >= 0 || bucketNeededForFilter) {
                bucket = decoder.readInt();
            } else {
                decoder.readInt();
                bucket = 0;
            }

            int totalBuckets;
            if (totalBucketsPosition >= 0 || bucketNeededForFilter) {
                totalBuckets = decoder.readInt();
            } else {
                decoder.readInt();
                totalBuckets = 0;
            }

            if (bucketFilter != null && !bucketFilter.test(partition, bucket, totalBuckets)) {
                skipFile(decoder);
                return false;
            }

            Object file;
            if (filePosition >= 0) {
                file = fileReader.read(decoder, row.getField(filePosition));
            } else {
                fileReader.skip(decoder);
                file = null;
            }

            setProjected(row, versionPosition, version);
            setProjected(row, kindPosition, (byte) kind);
            setProjected(row, partitionPosition, partitionBytes);
            setProjected(row, bucketPosition, bucket);
            setProjected(row, totalBucketsPosition, totalBuckets);
            setProjected(row, filePosition, file);
            return true;
        }

        private void skipBucketAndFile(AvroRecordDecoder decoder) throws IOException {
            decoder.readInt();
            decoder.readInt();
            skipFile(decoder);
        }

        private void skipFile(AvroRecordDecoder decoder) throws IOException {
            fileReader.skip(decoder);
        }

        private static void setProjected(
                GenericRow row, int outputPosition, @Nullable Object value) {
            if (outputPosition >= 0) {
                row.setField(outputPosition, value);
            }
        }

        private static void validateTopLevelFields(AvroRecordDecoder decoder) {
            if (decoder.fieldCount() != TOP_LEVEL_FIELDS.length) {
                throw new IllegalArgumentException(
                        String.format(
                                "Manifest Avro schema has %s top-level fields, expected %s.",
                                decoder.fieldCount(), TOP_LEVEL_FIELDS.length));
            }
            for (int i = 0; i < TOP_LEVEL_FIELDS.length; i++) {
                String actual = decoder.fieldName(i);
                String expected = TOP_LEVEL_FIELDS[i];
                if (!expected.equals(actual)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Unexpected Manifest Avro field at position %s: expected %s but found %s.",
                                    i, expected, actual));
                }
            }

            validateFieldType(decoder, 0, FieldType.INT);
            validateFieldType(decoder, 1, FieldType.INT);
            validateFieldType(decoder, 2, FieldType.BYTES);
            validateFieldType(decoder, 3, FieldType.INT);
            validateFieldType(decoder, 4, FieldType.INT);
            validateFieldType(decoder, 5, FieldType.RECORD);
        }

        private static void validateFieldType(
                AvroRecordDecoder decoder, int position, FieldType expectedType) {
            FieldType actualType = decoder.fieldType(position);
            if (actualType != expectedType) {
                throw new IllegalArgumentException(
                        String.format(
                                "Unexpected Manifest Avro type for field %s: expected %s but found %s.",
                                decoder.fieldName(position), expectedType, actualType));
            }
        }
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
        public RowIterator toRows(RowType projectedType) throws IOException {
            return toRows(projectedType, null, null, true);
        }

        private RowIterator toRows(
                RowType projectedType,
                @Nullable PartitionPredicate partitionFilter,
                @Nullable BucketFilter bucketFilter,
                boolean reuseRow)
                throws IOException {
            ManifestRecordDecoder recordDecoder = decoderContext.recordDecoder(projectedType);

            ByteBuffer decompressed = decoderContext.decompress(block);
            decoderContext.decoder.reset(
                    decompressed.array(),
                    decompressed.arrayOffset() + decompressed.position(),
                    decompressed.remaining());
            GenericRow reuse = reuseRow ? new GenericRow(recordDecoder.projectedFieldCount) : null;
            return new RowIterator(
                    blockRecordCount,
                    decoderContext.decoder,
                    recordDecoder,
                    reuse,
                    partitionFilter,
                    bucketFilter);
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

    /** Decoder state shared by the borrowed blocks produced by one reader. */
    private static final class DecoderContext {

        private final AvroRecordDecoder decoder;

        private @Nullable ByteBuffer decompressionBuffer;
        private RowType projectedRowType;
        private ManifestRecordDecoder recordDecoder;

        private DecoderContext(AvroRecordDecoder decoder) {
            this.decoder = decoder;
        }

        private ByteBuffer decompress(AvroRawBlock block) throws IOException {
            decompressionBuffer = block.decompress(decompressionBuffer);
            return decompressionBuffer;
        }

        private ManifestRecordDecoder recordDecoder(RowType rowType) {
            if (!rowType.equals(projectedRowType)) {
                recordDecoder = new ManifestRecordDecoder(decoder, rowType);
                projectedRowType = rowType;
            }
            return recordDecoder;
        }
    }

    /** Iterator over the reusable row decoded from one borrowed block. */
    public static final class RowIterator implements Iterator<GenericRow> {

        private final AvroRecordDecoder decoder;
        private final ManifestRecordDecoder recordDecoder;
        private final @Nullable GenericRow reuseRow;
        private GenericRow row;
        private final @Nullable PartitionPredicate partitionFilter;
        private final @Nullable BucketFilter bucketFilter;
        private final boolean filtered;

        private long blockRemaining;
        private long blockRecordIndex = -1;
        private boolean nextReady;
        private @Nullable ByteBuffer encodedRecord;

        private RowIterator(
                long recordCount,
                AvroRecordDecoder decoder,
                ManifestRecordDecoder recordDecoder,
                @Nullable GenericRow reuseRow,
                @Nullable PartitionPredicate partitionFilter,
                @Nullable BucketFilter bucketFilter) {
            blockRemaining = recordCount;
            this.decoder = decoder;
            this.recordDecoder = recordDecoder;
            this.reuseRow = reuseRow;
            this.row = reuseRow;
            this.partitionFilter = partitionFilter;
            this.bucketFilter = bucketFilter;
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
                    if (reuseRow == null) {
                        row = new GenericRow(recordDecoder.projectedFieldCount);
                    }
                    int recordStart = decoder.absolutePosition();
                    boolean selected =
                            recordDecoder.read(decoder, row, partitionFilter, bucketFilter);
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
                if (reuseRow == null) {
                    row = new GenericRow(recordDecoder.projectedFieldCount);
                }
                int recordStart = decoder.absolutePosition();
                recordDecoder.read(decoder, row, null, null);
                encodedRecord = decoder.borrowedView(recordStart, decoder.absolutePosition());
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
}
