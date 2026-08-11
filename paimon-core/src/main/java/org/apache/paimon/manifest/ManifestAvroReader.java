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
import org.apache.paimon.format.avro.AvroBlockReader.BorrowedBlock;
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
import java.util.NoSuchElementException;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/**
 * Schema-aware Avro reader which projects fields and filters before decoding data file metadata.
 */
final class ManifestAvroReader implements CloseableIterator<InternalRow> {

    private final AvroBlockReader blockReader;
    private final AvroRecordDecoder decoder;
    private final ManifestRecordDecoder recordDecoder;

    private long recordsRemaining;
    private @Nullable InternalRow next;
    private boolean nextReady;
    private boolean finished;

    ManifestAvroReader(
            InputStream input,
            RowType projectedType,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable BucketFilter bucketFilter)
            throws IOException {
        AvroBlockReader blockReader = new AvroBlockReader(input);
        try {
            AvroRecordDecoder decoder = blockReader.createRecordDecoder();
            this.recordDecoder =
                    new ManifestRecordDecoder(
                            decoder, projectedType, partitionFilter, bucketFilter);
            this.decoder = decoder;
            this.blockReader = blockReader;
        } catch (RuntimeException | Error e) {
            IOUtils.closeQuietly(blockReader);
            throw e;
        }
    }

    @Override
    public boolean hasNext() {
        if (nextReady) {
            return true;
        }
        if (finished) {
            return false;
        }

        try {
            while (true) {
                if (recordsRemaining == 0) {
                    if (decoder.isInitialized() && !decoder.isEnd()) {
                        throw new IOException(
                                "Manifest Avro block contains trailing undecoded bytes.");
                    }
                    if (!blockReader.hasNextBlock()) {
                        finished = true;
                        return false;
                    }
                    BorrowedBlock block = blockReader.nextBorrowedBlock();
                    decoder.reset(block.bytes(), block.offset(), block.length());
                    recordsRemaining = block.recordCount();
                }

                InternalRow candidate = recordDecoder.read(decoder);
                recordsRemaining--;
                if (candidate != null) {
                    next = candidate;
                    nextReady = true;
                    return true;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to decode Manifest Avro block.", e);
        }
    }

    @Override
    public InternalRow next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        InternalRow result = next;
        next = null;
        nextReady = false;
        return result;
    }

    long decodedDataFiles() {
        return recordDecoder.decodedDataFiles;
    }

    long skippedDataFiles() {
        return recordDecoder.skippedDataFiles;
    }

    @Override
    public void close() throws IOException {
        next = null;
        nextReady = false;
        finished = true;
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

        private final @Nullable PartitionPredicate partitionFilter;
        private final @Nullable BucketFilter bucketFilter;
        private final boolean partitionNeededForFilter;
        private final boolean bucketNeededForFilter;

        private long decodedDataFiles;
        private long skippedDataFiles;

        private ManifestRecordDecoder(
                AvroRecordDecoder decoder,
                RowType projectedType,
                @Nullable PartitionPredicate partitionFilter,
                @Nullable BucketFilter bucketFilter) {
            this.projectedFieldCount = projectedType.getFieldCount();
            this.versionPosition =
                    projectedType.getFieldIndex(ManifestSchemaUtils.FORMAT_IDENTIFIER_FIELD);
            this.kindPosition = projectedType.getFieldIndex(ManifestEntry.KIND);
            this.partitionPosition = projectedType.getFieldIndex(ManifestEntry.PARTITION);
            this.bucketPosition = projectedType.getFieldIndex(ManifestEntry.BUCKET);
            this.totalBucketsPosition = projectedType.getFieldIndex(ManifestEntry.TOTAL_BUCKETS);
            this.filePosition = projectedType.getFieldIndex(ManifestEntry.FILE);
            this.partitionFilter = partitionFilter;
            this.bucketFilter = bucketFilter;
            this.partitionNeededForFilter = partitionFilter != null || bucketFilter != null;
            this.bucketNeededForFilter = bucketFilter != null;

            validateTopLevelFields(decoder);
            // Manifest v2 has a fixed top-level layout, but DataFileMeta has gained nullable
            // fields. Build this reader from the writer schema so legacy files with fewer nested
            // fields still decode and expose the missing projected fields as null.
            fileReader =
                    decoder.createFieldDecoder(
                            5, filePosition >= 0 ? projectedType.getTypeAt(filePosition) : null);
        }

        private @Nullable InternalRow read(AvroRecordDecoder decoder) throws IOException {
            if (!decoder.readRecordStart()) {
                throw new IOException("Unexpected null or non-record Manifest Avro value.");
            }

            int version = decoder.readInt();
            ManifestEntrySerializer.checkFormatIdentifier(version);
            int kind = decoder.readInt();
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
                return null;
            }

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
                return null;
            }

            Object file;
            if (filePosition >= 0) {
                file = fileReader.read(decoder, null);
                decodedDataFiles++;
            } else {
                fileReader.skip(decoder);
                skippedDataFiles++;
                file = null;
            }

            GenericRow row = new GenericRow(projectedFieldCount);
            setProjected(row, versionPosition, version);
            setProjected(row, kindPosition, (byte) kind);
            setProjected(row, partitionPosition, partitionBytes);
            setProjected(row, bucketPosition, bucket);
            setProjected(row, totalBucketsPosition, totalBuckets);
            setProjected(row, filePosition, file);
            return row;
        }

        private void skipBucketAndFile(AvroRecordDecoder decoder) throws IOException {
            decoder.readInt();
            decoder.readInt();
            skipFile(decoder);
        }

        private void skipFile(AvroRecordDecoder decoder) throws IOException {
            fileReader.skip(decoder);
            skippedDataFiles++;
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
}
