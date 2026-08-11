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
import org.apache.paimon.format.avro.FieldReader;
import org.apache.paimon.format.avro.FieldReaderFactory;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.IOUtils;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

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
    private final ManifestRecordDecoder recordDecoder;

    private BinaryDecoder decoder;
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
            this.recordDecoder =
                    new ManifestRecordDecoder(
                            blockReader.schema(), projectedType, partitionFilter, bucketFilter);
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
                    if (decoder != null && !decoder.isEnd()) {
                        throw new IOException(
                                "Manifest Avro block contains trailing undecoded bytes.");
                    }
                    if (!blockReader.hasNextBlock()) {
                        finished = true;
                        return false;
                    }
                    byte[] block = blockReader.nextBlock();
                    decoder = DecoderFactory.get().binaryDecoder(block, decoder);
                    recordsRemaining = blockReader.currentBlockRecordCount();
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

        private final int topLevelRecordIndex;
        private final int projectedFieldCount;
        private final int versionPosition;
        private final int kindPosition;
        private final int partitionPosition;
        private final int bucketPosition;
        private final int totalBucketsPosition;
        private final int filePosition;

        private final FieldReader versionReader;
        private final FieldReader kindReader;
        private final FieldReader partitionReader;
        private final FieldReader bucketReader;
        private final FieldReader totalBucketsReader;
        private final FieldReader fileReader;

        private final @Nullable PartitionPredicate partitionFilter;
        private final @Nullable BucketFilter bucketFilter;
        private final boolean partitionNeededForFilter;
        private final boolean bucketNeededForFilter;

        private long decodedDataFiles;
        private long skippedDataFiles;

        private ManifestRecordDecoder(
                Schema writerSchema,
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

            Schema recordSchema;
            if (writerSchema.getType() == Schema.Type.UNION) {
                int recordIndex = -1;
                Schema record = null;
                for (int i = 0; i < writerSchema.getTypes().size(); i++) {
                    Schema branch = writerSchema.getTypes().get(i);
                    if (branch.getType() == Schema.Type.RECORD) {
                        if (record != null) {
                            throw new IllegalArgumentException(
                                    "Manifest Avro union contains multiple record branches.");
                        }
                        record = branch;
                        recordIndex = i;
                    }
                }
                if (record == null) {
                    throw new IllegalArgumentException(
                            "Manifest Avro schema is not a record or record union.");
                }
                recordSchema = record;
                topLevelRecordIndex = recordIndex;
            } else if (writerSchema.getType() == Schema.Type.RECORD) {
                recordSchema = writerSchema;
                topLevelRecordIndex = -1;
            } else {
                throw new IllegalArgumentException(
                        "Manifest Avro schema is not a record or record union.");
            }

            validateTopLevelFields(recordSchema);
            FieldReaderFactory factory = new FieldReaderFactory();
            versionReader =
                    createReader(factory, recordSchema, 0, projectedType, versionPosition, null);
            kindReader = createReader(factory, recordSchema, 1, projectedType, kindPosition, null);
            partitionReader =
                    createReader(
                            factory,
                            recordSchema,
                            2,
                            projectedType,
                            partitionPosition,
                            partitionNeededForFilter
                                    ? ManifestEntry.MANIFEST_ROW_TYPE
                                            .getField(ManifestEntry.PARTITION)
                                            .type()
                                    : null);
            bucketReader =
                    createReader(
                            factory,
                            recordSchema,
                            3,
                            projectedType,
                            bucketPosition,
                            bucketNeededForFilter
                                    ? ManifestEntry.MANIFEST_ROW_TYPE
                                            .getField(ManifestEntry.BUCKET)
                                            .type()
                                    : null);
            totalBucketsReader =
                    createReader(
                            factory,
                            recordSchema,
                            4,
                            projectedType,
                            totalBucketsPosition,
                            bucketNeededForFilter
                                    ? ManifestEntry.MANIFEST_ROW_TYPE
                                            .getField(ManifestEntry.TOTAL_BUCKETS)
                                            .type()
                                    : null);
            // Manifest v2 has a fixed top-level layout, but DataFileMeta has gained nullable
            // fields. Build this reader from the writer schema so legacy files with fewer nested
            // fields still decode and expose the missing projected fields as null.
            fileReader = createReader(factory, recordSchema, 5, projectedType, filePosition, null);
        }

        private @Nullable InternalRow read(BinaryDecoder decoder) throws IOException {
            if (topLevelRecordIndex >= 0) {
                int branch = decoder.readIndex();
                if (branch != topLevelRecordIndex) {
                    throw new IOException("Unexpected null or non-record Manifest Avro value.");
                }
            }

            Object version = readProjected(versionReader, versionPosition, decoder);
            Object kind = readProjected(kindReader, kindPosition, decoder);
            byte[] partitionBytes;
            if (partitionPosition >= 0 || partitionNeededForFilter) {
                partitionBytes = (byte[]) partitionReader.read(decoder, null);
            } else {
                partitionReader.skip(decoder);
                partitionBytes = null;
            }

            BinaryRow partition =
                    partitionNeededForFilter ? deserializeBinaryRow(partitionBytes) : null;
            if (partitionFilter != null && !partitionFilter.test(partition)) {
                skipBucketAndFile(decoder);
                return null;
            }

            Integer bucket;
            if (bucketPosition >= 0 || bucketNeededForFilter) {
                bucket = (Integer) bucketReader.read(decoder, null);
            } else {
                bucketReader.skip(decoder);
                bucket = null;
            }

            Integer totalBuckets;
            if (totalBucketsPosition >= 0 || bucketNeededForFilter) {
                totalBuckets = (Integer) totalBucketsReader.read(decoder, null);
            } else {
                totalBucketsReader.skip(decoder);
                totalBuckets = null;
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
            setProjected(row, kindPosition, kind);
            setProjected(row, partitionPosition, partitionBytes);
            setProjected(row, bucketPosition, bucket);
            setProjected(row, totalBucketsPosition, totalBuckets);
            setProjected(row, filePosition, file);
            return row;
        }

        private void skipBucketAndFile(BinaryDecoder decoder) throws IOException {
            bucketReader.skip(decoder);
            totalBucketsReader.skip(decoder);
            skipFile(decoder);
        }

        private void skipFile(BinaryDecoder decoder) throws IOException {
            fileReader.skip(decoder);
            skippedDataFiles++;
        }

        private static @Nullable Object readProjected(
                FieldReader reader, int outputPosition, BinaryDecoder decoder) throws IOException {
            if (outputPosition >= 0) {
                return reader.read(decoder, null);
            }
            reader.skip(decoder);
            return null;
        }

        private static void setProjected(
                GenericRow row, int outputPosition, @Nullable Object value) {
            if (outputPosition >= 0) {
                row.setField(outputPosition, value);
            }
        }

        private static FieldReader createReader(
                FieldReaderFactory factory,
                Schema recordSchema,
                int writerPosition,
                RowType projectedType,
                int outputPosition,
                @Nullable DataType requiredType) {
            DataType readType =
                    outputPosition >= 0 ? projectedType.getTypeAt(outputPosition) : requiredType;
            return factory.visit(recordSchema.getFields().get(writerPosition).schema(), readType);
        }

        private static void validateTopLevelFields(Schema recordSchema) {
            if (recordSchema.getFields().size() != TOP_LEVEL_FIELDS.length) {
                throw new IllegalArgumentException(
                        String.format(
                                "Manifest Avro schema has %s top-level fields, expected %s.",
                                recordSchema.getFields().size(), TOP_LEVEL_FIELDS.length));
            }
            for (int i = 0; i < TOP_LEVEL_FIELDS.length; i++) {
                String actual = recordSchema.getFields().get(i).name();
                String expected = TOP_LEVEL_FIELDS[i];
                if (!expected.equals(actual)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Unexpected Manifest Avro field at position %s: expected %s but found %s.",
                                    i, expected, actual));
                }
            }
        }
    }
}
