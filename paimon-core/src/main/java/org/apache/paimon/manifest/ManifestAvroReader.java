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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.avro.AvroBlockReader;
import org.apache.paimon.format.avro.AvroFileFormat;
import org.apache.paimon.format.avro.AvroRawBlock;
import org.apache.paimon.format.avro.AvroRecordDecoder;
import org.apache.paimon.format.avro.AvroRecordDecoder.FieldDecoder;
import org.apache.paimon.format.avro.AvroRecordDecoder.FieldType;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

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

    ManifestAvroReader(InputStream input, AvroFileFormat avroFileFormat, String compression)
            throws IOException {
        AvroBlockReader stream = null;
        try {
            stream = new AvroBlockReader(input);
            this.stream = stream;
            this.decoderContext = new DecoderContext(stream.createRecordDecoder());
            this.rawBlockCopySupported =
                    stream.supportsRawBlockCopy(
                            avroFileFormat, ManifestEntry.MANIFEST_ROW_TYPE, compression);
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
            BlockRow row = recordDecoder.createRow(decompressed.array());
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
    public static final class RowIterator implements Iterator<BlockRow> {

        private final AvroRecordDecoder decoder;
        private final ManifestEntryDecoder recordDecoder;
        private final BlockRow row;
        private final @Nullable PartitionPredicate partitionFilter;
        private final @Nullable BucketFilter bucketFilter;
        private final @Nullable ReadStatistics readStatistics;
        private final boolean filtered;

        private long blockRemaining;
        private long blockRecordIndex = -1;
        private boolean nextReady;

        private RowIterator(
                long recordCount,
                AvroRecordDecoder decoder,
                ManifestEntryDecoder recordDecoder,
                BlockRow row,
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
                    boolean selected =
                            recordDecoder.read(decoder, row, partitionFilter, bucketFilter);
                    recordRead(selected);
                    blockRemaining--;
                    if (selected) {
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
        public BlockRow next() {
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
                recordDecoder.read(decoder, row, null, null);
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

        private void recordRead(boolean selected) {
            if (readStatistics != null) {
                if (selected && recordDecoder.fileDecoder != null) {
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

    /** Reusable {@link GenericRow} with borrowed views over fields in the current Avro block. */
    public static final class BlockRow extends GenericRow {

        private final RowType rowType;
        private final @Nullable BlockRow fileRow;
        private final ByteBuffer[] byteBuffers;
        private final BinaryString[] stringViews;
        private final MemorySegment[] blockSegments;
        private @Nullable BinaryRow binaryRowView;
        private ByteBuffer encodedRecord;

        private BlockRow(
                RowType rowType, int filePosition, @Nullable RowType fileType, byte[] blockBytes) {
            super(rowType.getFieldCount());
            this.rowType = rowType;
            this.byteBuffers = new ByteBuffer[rowType.getFieldCount()];
            this.stringViews = new BinaryString[rowType.getFieldCount()];
            this.blockSegments = new MemorySegment[] {MemorySegment.wrap(blockBytes)};
            this.fileRow = fileType == null ? null : new BlockRow(fileType, blockSegments);
            if (fileRow != null) {
                setField(filePosition, fileRow);
            }
        }

        private BlockRow(RowType rowType, MemorySegment[] blockSegments) {
            super(rowType.getFieldCount());
            this.rowType = rowType;
            this.fileRow = null;
            this.byteBuffers = new ByteBuffer[rowType.getFieldCount()];
            this.stringViews = new BinaryString[rowType.getFieldCount()];
            this.blockSegments = blockSegments;
        }

        public ByteBuffer getByteBuffer(int pos) {
            if (isNullAt(pos)) {
                throw new IllegalStateException("Manifest field is null.");
            }
            ByteBuffer bytes = byteBuffers[pos];
            if (bytes == null) {
                throw new IllegalStateException("Manifest field has no raw Avro byte view.");
            }
            return bytes;
        }

        private BinaryRow binaryRow(ByteBuffer bytes) {
            int arity = bytes.getInt(bytes.position());
            if (binaryRowView == null || binaryRowView.getFieldCount() != arity) {
                binaryRowView = new BinaryRow(arity);
            }
            binaryRowView.pointTo(
                    blockSegments[0],
                    bytes.arrayOffset() + bytes.position() + Integer.BYTES,
                    bytes.remaining() - Integer.BYTES);
            return binaryRowView;
        }

        /** Returns the reusable encoded view of the current complete Avro record. */
        public ByteBuffer encodedRecord() {
            if (encodedRecord == null) {
                throw new IllegalStateException("No current Manifest Avro record.");
            }
            return encodedRecord;
        }

        @Override
        public byte[] getBinary(int pos) {
            ByteBuffer bytes = getByteBuffer(pos);
            return Arrays.copyOfRange(
                    bytes.array(),
                    bytes.arrayOffset() + bytes.position(),
                    bytes.arrayOffset() + bytes.limit());
        }

        private void setByteBuffer(int pos, ByteBuffer source) {
            ByteBuffer target = byteBuffers[pos];
            if (target == null || target.array() != source.array()) {
                target = ByteBuffer.wrap(source.array());
                byteBuffers[pos] = target;
            }
            target.clear();
            target.position(source.arrayOffset() + source.position());
            target.limit(source.arrayOffset() + source.limit());

            switch (rowType.getTypeAt(pos).getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    BinaryString string = stringViews[pos];
                    if (string == null) {
                        string =
                                new BinaryString(
                                        blockSegments,
                                        target.arrayOffset() + target.position(),
                                        target.remaining());
                        stringViews[pos] = string;
                    } else {
                        string.pointTo(
                                blockSegments,
                                target.arrayOffset() + target.position(),
                                target.remaining());
                    }
                    setField(pos, string);
                    break;
                case BINARY:
                case VARBINARY:
                    setField(pos, target);
                    break;
                default:
                    break;
            }
        }

        private void setEncodedRecord(AvroRecordDecoder decoder, int start, int end) {
            encodedRecord = decoder.borrowedView(start, end);
        }
    }

    private static final class ManifestEntryDecoder {

        private final RowType projectedType;
        private final int versionPosition;
        private final int kindPosition;
        private final int partitionPosition;
        private final int bucketPosition;
        private final int totalBucketsPosition;
        private final int filePosition;
        private final @Nullable RowType projectedFileType;
        private final @Nullable DataFileDecoder fileDecoder;
        private final FieldDecoder fileSkipper;

        private ManifestEntryDecoder(AvroRecordDecoder decoder, RowType projectedType) {
            validateTopLevelSchema(decoder);
            this.projectedType = projectedType;
            this.filePosition = projectedType.getFieldIndex(ManifestEntry.FILE);
            this.projectedFileType =
                    filePosition < 0 ? null : (RowType) projectedType.getTypeAt(filePosition);
            this.versionPosition =
                    projectedType.getFieldIndex(ManifestSchemaUtils.FORMAT_IDENTIFIER_FIELD);
            this.kindPosition = projectedType.getFieldIndex(ManifestEntry.KIND);
            this.partitionPosition = projectedType.getFieldIndex(ManifestEntry.PARTITION);
            this.bucketPosition = projectedType.getFieldIndex(ManifestEntry.BUCKET);
            this.totalBucketsPosition = projectedType.getFieldIndex(ManifestEntry.TOTAL_BUCKETS);

            this.fileSkipper = decoder.createFieldDecoder(5, null);
            this.fileDecoder =
                    projectedFileType == null
                            ? null
                            : new DataFileDecoder(decoder, projectedFileType);
        }

        private BlockRow createRow(byte[] blockBytes) {
            return new BlockRow(projectedType, filePosition, projectedFileType, blockBytes);
        }

        private boolean read(
                AvroRecordDecoder decoder,
                BlockRow row,
                @Nullable PartitionPredicate partitionFilter,
                @Nullable BucketFilter bucketFilter)
                throws IOException {
            int recordStart = decoder.absolutePosition();
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
            ByteBuffer partitionBytes;
            if (partitionPosition < 0 && !partitionNeededForFilter) {
                decoder.skipBytes();
                partitionBytes = null;
            } else {
                partitionBytes = decoder.readBytesView();
                if (partitionPosition >= 0) {
                    row.setByteBuffer(partitionPosition, partitionBytes);
                }
            }

            BinaryRow partition = null;
            if (partitionNeededForFilter) {
                partition = row.binaryRow(partitionBytes);
                if (partitionFilter != null && !partitionFilter.test(partition)) {
                    decoder.readInt();
                    decoder.readInt();
                    fileSkipper.skip(decoder);
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
                fileSkipper.skip(decoder);
                return false;
            }

            if (fileDecoder == null) {
                fileSkipper.skip(decoder);
            } else {
                fileDecoder.read(decoder, row.fileRow);
            }
            row.setEncodedRecord(decoder, recordStart, decoder.absolutePosition());
            return true;
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

    private static final class DataFileDecoder {

        private static final int MINIMUM_FIELD_COUNT = 13;

        private final int writerFieldCount;
        private final int fileNameIndex;
        private final int fileSizeIndex;
        private final int rowCountIndex;
        private final int minKeyIndex;
        private final int maxKeyIndex;
        private final int keyStatsIndex;
        private final int valueStatsIndex;
        private final int minSequenceNumberIndex;
        private final int maxSequenceNumberIndex;
        private final int schemaIdIndex;
        private final int levelIndex;
        private final int extraFilesIndex;
        private final int creationTimeIndex;
        private final int deleteRowCountIndex;
        private final int embeddedFileIndex;
        private final int fileSourceIndex;
        private final int valueStatsColsIndex;
        private final int externalPathIndex;
        private final int firstRowIdIndex;
        private final int writeColsIndex;

        private final FieldDecoder keyStatsDecoder;
        private final FieldDecoder valueStatsDecoder;
        private final FieldDecoder extraFilesDecoder;
        private final FieldDecoder creationTimeDecoder;
        private final @Nullable FieldDecoder valueStatsColsDecoder;
        private final @Nullable FieldDecoder writeColsDecoder;

        private DataFileDecoder(AvroRecordDecoder decoder, RowType projectedType) {
            writerFieldCount = decoder.nestedFieldCount(5);
            if (writerFieldCount < MINIMUM_FIELD_COUNT
                    || writerFieldCount > DataFileMeta.SCHEMA.getFieldCount()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Unsupported Manifest Avro data file field count %s.",
                                writerFieldCount));
            }
            for (int position = 0; position < writerFieldCount; position++) {
                String actual = decoder.nestedFieldName(5, position);
                String expected = DataFileMeta.SCHEMA.getField(position).name();
                if (!actual.equals(expected)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Unexpected Manifest Avro data file field at position %s: expected %s but found %s.",
                                    position, expected, actual));
                }
            }

            fileNameIndex = projectedType.getFieldIndex(DataFileMeta.FILE_NAME);
            fileSizeIndex = projectedType.getFieldIndex(DataFileMeta.FILE_SIZE);
            rowCountIndex = projectedType.getFieldIndex(DataFileMeta.ROW_COUNT);
            minKeyIndex = projectedType.getFieldIndex(DataFileMeta.MIN_KEY);
            maxKeyIndex = projectedType.getFieldIndex(DataFileMeta.MAX_KEY);
            keyStatsIndex = projectedType.getFieldIndex(DataFileMeta.KEY_STATS);
            valueStatsIndex = projectedType.getFieldIndex(DataFileMeta.VALUE_STATS);
            minSequenceNumberIndex = projectedType.getFieldIndex(DataFileMeta.MIN_SEQUENCE_NUMBER);
            maxSequenceNumberIndex = projectedType.getFieldIndex(DataFileMeta.MAX_SEQUENCE_NUMBER);
            schemaIdIndex = projectedType.getFieldIndex(DataFileMeta.SCHEMA_ID);
            levelIndex = projectedType.getFieldIndex(DataFileMeta.LEVEL);
            extraFilesIndex = projectedType.getFieldIndex(DataFileMeta.EXTRA_FILES);
            creationTimeIndex = projectedType.getFieldIndex(DataFileMeta.CREATION_TIME);
            deleteRowCountIndex = projectedType.getFieldIndex(DataFileMeta.DELETE_ROW_COUNT);
            embeddedFileIndex = projectedType.getFieldIndex(DataFileMeta.EMBEDDED_FILE_INDEX);
            fileSourceIndex = projectedType.getFieldIndex(DataFileMeta.FILE_SOURCE);
            valueStatsColsIndex = projectedType.getFieldIndex(DataFileMeta.VALUE_STATS_COLS);
            externalPathIndex = projectedType.getFieldIndex(DataFileMeta.EXTERNAL_PATH);
            firstRowIdIndex = projectedType.getFieldIndex(DataFileMeta.FIRST_ROW_ID);
            writeColsIndex = projectedType.getFieldIndex(DataFileMeta.WRITE_COLS);

            keyStatsDecoder =
                    decoder.createNestedFieldDecoder(
                            5,
                            5,
                            keyStatsIndex < 0 ? null : projectedType.getTypeAt(keyStatsIndex));
            valueStatsDecoder =
                    decoder.createNestedFieldDecoder(
                            5,
                            6,
                            valueStatsIndex < 0 ? null : projectedType.getTypeAt(valueStatsIndex));
            extraFilesDecoder =
                    decoder.createNestedFieldDecoder(
                            5,
                            11,
                            extraFilesIndex < 0 ? null : projectedType.getTypeAt(extraFilesIndex));
            creationTimeDecoder =
                    decoder.createNestedFieldDecoder(
                            5,
                            12,
                            creationTimeIndex < 0
                                    ? null
                                    : projectedType.getTypeAt(creationTimeIndex));
            valueStatsColsDecoder =
                    writerFieldCount <= 16
                            ? null
                            : decoder.createNestedFieldDecoder(
                                    5,
                                    16,
                                    valueStatsColsIndex < 0
                                            ? null
                                            : projectedType.getTypeAt(valueStatsColsIndex));
            writeColsDecoder =
                    writerFieldCount <= 19
                            ? null
                            : decoder.createNestedFieldDecoder(
                                    5,
                                    19,
                                    writeColsIndex < 0
                                            ? null
                                            : projectedType.getTypeAt(writeColsIndex));
        }

        private void read(AvroRecordDecoder decoder, BlockRow row) throws IOException {
            if (fileNameIndex < 0) {
                decoder.skipBytes();
            } else {
                row.setByteBuffer(fileNameIndex, decoder.readBytesView());
            }

            long fileSize = decoder.readLong();
            if (fileSizeIndex >= 0) {
                row.setField(fileSizeIndex, fileSize);
            }

            long rowCount = decoder.readLong();
            if (rowCountIndex >= 0) {
                row.setField(rowCountIndex, rowCount);
            }

            if (minKeyIndex < 0) {
                decoder.skipBytes();
            } else {
                row.setByteBuffer(minKeyIndex, decoder.readBytesView());
            }

            if (maxKeyIndex < 0) {
                decoder.skipBytes();
            } else {
                row.setByteBuffer(maxKeyIndex, decoder.readBytesView());
            }

            if (keyStatsIndex < 0) {
                keyStatsDecoder.skip(decoder);
            } else {
                row.setField(
                        keyStatsIndex, keyStatsDecoder.read(decoder, row.getField(keyStatsIndex)));
            }

            if (valueStatsIndex < 0) {
                valueStatsDecoder.skip(decoder);
            } else {
                row.setField(
                        valueStatsIndex,
                        valueStatsDecoder.read(decoder, row.getField(valueStatsIndex)));
            }

            long minSequenceNumber = decoder.readLong();
            if (minSequenceNumberIndex >= 0) {
                row.setField(minSequenceNumberIndex, minSequenceNumber);
            }

            long maxSequenceNumber = decoder.readLong();
            if (maxSequenceNumberIndex >= 0) {
                row.setField(maxSequenceNumberIndex, maxSequenceNumber);
            }

            long schemaId = decoder.readLong();
            if (schemaIdIndex >= 0) {
                row.setField(schemaIdIndex, schemaId);
            }

            int level = decoder.readInt();
            if (levelIndex >= 0) {
                row.setField(levelIndex, level);
            }

            if (extraFilesIndex < 0) {
                extraFilesDecoder.skip(decoder);
            } else {
                int start = decoder.absolutePosition();
                row.setField(
                        extraFilesIndex,
                        extraFilesDecoder.read(decoder, row.getField(extraFilesIndex)));
                row.setByteBuffer(
                        extraFilesIndex, decoder.borrowedView(start, decoder.absolutePosition()));
            }

            if (creationTimeIndex < 0) {
                creationTimeDecoder.skip(decoder);
            } else {
                row.setField(
                        creationTimeIndex,
                        creationTimeDecoder.read(decoder, row.getField(creationTimeIndex)));
            }

            if (writerFieldCount > 13) {
                int branch = decoder.readIndex();
                if (branch == 0) {
                    if (deleteRowCountIndex >= 0) {
                        row.setField(deleteRowCountIndex, null);
                    }
                } else if (branch == 1) {
                    long deleteRowCount = decoder.readLong();
                    if (deleteRowCountIndex >= 0) {
                        row.setField(deleteRowCountIndex, deleteRowCount);
                    }
                } else if (branch != 0) {
                    throw new IOException(
                            "Invalid nullable delete row count union branch " + branch);
                }
            }

            if (writerFieldCount > 14) {
                int branch = decoder.readIndex();
                if (branch == 0) {
                    if (embeddedFileIndex >= 0) {
                        row.setField(embeddedFileIndex, null);
                    }
                } else if (branch == 1) {
                    if (embeddedFileIndex < 0) {
                        decoder.skipBytes();
                    } else {
                        row.setByteBuffer(embeddedFileIndex, decoder.readBytesView());
                    }
                } else if (branch != 0) {
                    throw new IOException(
                            "Invalid nullable embedded file index union branch " + branch);
                }
            }

            if (writerFieldCount > 15) {
                int branch = decoder.readIndex();
                if (branch == 0) {
                    if (fileSourceIndex >= 0) {
                        row.setField(fileSourceIndex, null);
                    }
                } else if (branch == 1) {
                    int fileSource = decoder.readInt();
                    if (fileSourceIndex >= 0) {
                        row.setField(fileSourceIndex, (byte) fileSource);
                    }
                } else if (branch != 0) {
                    throw new IOException("Invalid nullable file source union branch " + branch);
                }
            }

            if (writerFieldCount > 16) {
                if (valueStatsColsIndex < 0) {
                    valueStatsColsDecoder.skip(decoder);
                } else {
                    row.setField(
                            valueStatsColsIndex,
                            valueStatsColsDecoder.read(decoder, row.getField(valueStatsColsIndex)));
                }
            }

            if (writerFieldCount > 17) {
                int branch = decoder.readIndex();
                if (branch == 0) {
                    if (externalPathIndex >= 0) {
                        row.setField(externalPathIndex, null);
                    }
                } else if (branch == 1) {
                    if (externalPathIndex < 0) {
                        decoder.skipBytes();
                    } else {
                        row.setByteBuffer(externalPathIndex, decoder.readBytesView());
                    }
                } else if (branch != 0) {
                    throw new IOException("Invalid nullable external path union branch " + branch);
                }
            }

            if (writerFieldCount > 18) {
                int branch = decoder.readIndex();
                if (branch == 0) {
                    if (firstRowIdIndex >= 0) {
                        row.setField(firstRowIdIndex, null);
                    }
                } else if (branch == 1) {
                    long firstRowId = decoder.readLong();
                    if (firstRowIdIndex >= 0) {
                        row.setField(firstRowIdIndex, firstRowId);
                    }
                } else if (branch != 0) {
                    throw new IOException("Invalid nullable first row id union branch " + branch);
                }
            }

            if (writerFieldCount > 19) {
                if (writeColsIndex < 0) {
                    writeColsDecoder.skip(decoder);
                } else {
                    row.setField(
                            writeColsIndex,
                            writeColsDecoder.read(decoder, row.getField(writeColsIndex)));
                }
            }
        }
    }
}
