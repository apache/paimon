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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.avro.AvroBlockReader;
import org.apache.paimon.format.avro.AvroRawBlock;
import org.apache.paimon.format.avro.AvroRecordDecoder;
import org.apache.paimon.format.avro.AvroRecordDecoder.FieldDecoder;
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

/** Schema-aware Avro reader for projected index manifest rows and encoded records. */
public final class IndexManifestAvroReader implements AutoCloseable {

    private final AvroBlockReader blockReader;
    private final DecoderContext decoderContext;
    private final boolean rawBlockCopySupported;

    private long blockOrdinal = -1;

    IndexManifestAvroReader(InputStream input) throws IOException {
        AvroBlockReader blockReader = null;
        try {
            blockReader = new AvroBlockReader(input);
            this.blockReader = blockReader;
            this.decoderContext = new DecoderContext(blockReader.createRecordDecoder());
            this.rawBlockCopySupported =
                    blockReader.supportsRawBlockCopy(IndexManifestEntry.MANIFEST_ROW_TYPE);
        } catch (IOException | RuntimeException | Error failure) {
            IOUtils.closeQuietly(blockReader == null ? input : blockReader);
            throw failure;
        }
    }

    /** Returns whether encoded records and raw blocks use the current index manifest layout. */
    public boolean rawBlockCopySupported() {
        return rawBlockCopySupported;
    }

    /** Returns whether another raw Avro block is available. */
    public boolean hasNext() throws IOException {
        return blockReader.hasNextBlock();
    }

    /** Returns the next borrowed raw block. */
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

    /** Reads independent projected rows from all remaining blocks. */
    public CloseableIterator<InternalRow> read(RowType projectedType) {
        return read(projectedType, false);
    }

    /** Reads projected rows, optionally reusing one row until the iterator advances. */
    CloseableIterator<InternalRow> read(RowType projectedType, boolean reuseRow) {
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
                        if (!IndexManifestAvroReader.this.hasNext()) {
                            return false;
                        }
                        rows = IndexManifestAvroReader.this.next().toRows(projectedType, reuseRow);
                    }
                    return true;
                } catch (IOException e) {
                    throw new UncheckedIOException(
                            "Failed to decode Index Manifest Avro block.", e);
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
                IndexManifestAvroReader.this.close();
            }
        };
    }

    @Override
    public void close() throws IOException {
        blockReader.close();
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
            return toRows(projectedType, true);
        }

        private RowIterator toRows(RowType projectedType, boolean reuseRow) throws IOException {
            IndexRecordDecoder recordDecoder = decoderContext.recordDecoder(projectedType);
            ByteBuffer decompressed = decoderContext.decompress(block);
            decoderContext.decoder.reset(
                    decompressed.array(),
                    decompressed.arrayOffset() + decompressed.position(),
                    decompressed.remaining());
            return new RowIterator(
                    blockRecordCount,
                    decoderContext.decoder,
                    recordDecoder,
                    reuseRow ? new GenericRow(recordDecoder.projectedFieldCount) : null);
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

    private static final class DecoderContext {

        private final AvroRecordDecoder decoder;

        private @Nullable ByteBuffer decompressionBuffer;
        private @Nullable RowType projectedRowType;
        private @Nullable IndexRecordDecoder recordDecoder;

        private DecoderContext(AvroRecordDecoder decoder) {
            this.decoder = decoder;
        }

        private ByteBuffer decompress(AvroRawBlock block) throws IOException {
            decompressionBuffer = block.decompress(decompressionBuffer);
            return decompressionBuffer;
        }

        private IndexRecordDecoder recordDecoder(RowType rowType) {
            if (!rowType.equals(projectedRowType)) {
                recordDecoder = new IndexRecordDecoder(decoder, rowType);
                projectedRowType = rowType;
            }
            return recordDecoder;
        }
    }

    private static final class IndexRecordDecoder {

        private final int projectedFieldCount;
        private final int[] outputPositions;
        private final FieldDecoder[] fieldDecoders;
        private final int[] missingOutputPositions;

        private IndexRecordDecoder(AvroRecordDecoder decoder, RowType projectedType) {
            this.projectedFieldCount = projectedType.getFieldCount();
            this.outputPositions = new int[decoder.fieldCount()];
            this.fieldDecoders = new FieldDecoder[decoder.fieldCount()];
            boolean[] present = new boolean[projectedFieldCount];
            for (int i = 0; i < decoder.fieldCount(); i++) {
                int outputPosition = projectedType.getFieldIndex(decoder.fieldName(i));
                outputPositions[i] = outputPosition;
                if (outputPosition >= 0) {
                    present[outputPosition] = true;
                }
                fieldDecoders[i] =
                        decoder.createFieldDecoder(
                                i,
                                outputPosition < 0
                                        ? null
                                        : projectedType.getTypeAt(outputPosition));
            }

            int missingCount = 0;
            for (boolean fieldPresent : present) {
                if (!fieldPresent) {
                    missingCount++;
                }
            }
            this.missingOutputPositions = new int[missingCount];
            int missingIndex = 0;
            for (int i = 0; i < present.length; i++) {
                if (!present[i]) {
                    missingOutputPositions[missingIndex++] = i;
                }
            }
        }

        private void read(AvroRecordDecoder decoder, GenericRow row) throws IOException {
            if (!decoder.readRecordStart()) {
                throw new IOException("Unexpected null or non-record Index Manifest Avro value.");
            }
            for (int i = 0; i < fieldDecoders.length; i++) {
                int outputPosition = outputPositions[i];
                if (outputPosition < 0) {
                    fieldDecoders[i].skip(decoder);
                } else {
                    row.setField(
                            outputPosition,
                            fieldDecoders[i].read(decoder, row.getField(outputPosition)));
                }
            }
            for (int missingOutputPosition : missingOutputPositions) {
                row.setField(missingOutputPosition, null);
            }
        }
    }

    /** Iterator over decoded rows and their borrowed encoded Avro records. */
    public static final class RowIterator implements Iterator<GenericRow> {

        private final AvroRecordDecoder decoder;
        private final IndexRecordDecoder recordDecoder;
        private final @Nullable GenericRow reuseRow;

        private long blockRemaining;
        private long blockRecordIndex = -1;
        private @Nullable ByteBuffer encodedRecord;

        private RowIterator(
                long recordCount,
                AvroRecordDecoder decoder,
                IndexRecordDecoder recordDecoder,
                @Nullable GenericRow reuseRow) {
            this.blockRemaining = recordCount;
            this.decoder = decoder;
            this.recordDecoder = recordDecoder;
            this.reuseRow = reuseRow;
        }

        @Override
        public boolean hasNext() {
            try {
                ensureBlockFullyConsumed();
                return blockRemaining > 0;
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Failed to decode projected Index Manifest Avro record.", e);
            }
        }

        @Override
        public GenericRow next() {
            if (blockRemaining == 0) {
                throw new NoSuchElementException();
            }
            try {
                blockRecordIndex++;
                GenericRow row =
                        reuseRow == null
                                ? new GenericRow(recordDecoder.projectedFieldCount)
                                : reuseRow;
                int recordStart = decoder.absolutePosition();
                recordDecoder.read(decoder, row);
                encodedRecord = decoder.borrowedView(recordStart, decoder.absolutePosition());
                blockRemaining--;
                return row;
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Failed to decode projected Index Manifest Avro record.", e);
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
                throw new IllegalStateException("No current Index Manifest Avro record.");
            }
            return encodedRecord;
        }

        private void ensureBlockFullyConsumed() throws IOException {
            if (blockRemaining == 0 && !decoder.isEnd()) {
                throw new IOException("Index Manifest Avro block contains trailing bytes.");
            }
        }

        private void checkCurrentRecord() {
            if (blockRecordIndex < 0) {
                throw new IllegalStateException("No current Index Manifest Avro record.");
            }
        }
    }
}
