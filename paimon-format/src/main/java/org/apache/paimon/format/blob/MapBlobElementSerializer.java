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

package org.apache.paimon.format.blob;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobFetchMetricReporter;
import org.apache.paimon.data.BlobMapPlaceholder;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.utils.DeltaVarintCompressor;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.StreamUtils.intToLittleEndian;
import static org.apache.paimon.utils.StreamUtils.longToLittleEndian;

/**
 * Element serializer for a MAP&lt;X, BLOB&gt; field.
 *
 * <p>The element payload, excluding the common record framing, is laid out as follows:
 *
 * <pre>
 * +-------------------+-------------+-----------------+
 * | map magic (4 B)   | version (1) | entry count (4) |
 * +-------------------+-------------+-----------------+
 * | concatenated non-null key bytes                   |
 * +---------------------------------------------------+
 * | concatenated non-null Blob bytes                  |
 * +---------------------------------------------------+
 * | delta-varint compressed key lengths               |
 * +---------------------------------------------------+
 * | delta-varint compressed Blob lengths              |
 * +------------------------+--------------------------+
 * |  key index size (4 B)  |  Blob index size (4 B)   |
 * +------------------------+--------------------------+
 * </pre>
 *
 * <p>The key and Blob length arrays are aligned by entry ordinal. A length of {@code -1} represents
 * null; zero represents an empty key or Blob.
 */
final class MapBlobElementSerializer implements BlobElementSerializer {

    static final byte VERSION = 1;
    static final int MAGIC_NUMBER = 0x4D424342;
    static final byte[] MAGIC_NUMBER_BYTES = intToLittleEndian(MAGIC_NUMBER);
    static final long NULL_VALUE_LENGTH = -1L;

    private static final int HEADER_LENGTH = 9;
    private static final int INDEX_LENGTH_SIZE = Integer.BYTES;
    private static final int INDEX_LENGTHS_SIZE = INDEX_LENGTH_SIZE * 2;
    private static final long NULL_KEY_LENGTH = -1L;
    private static final int MIN_PAYLOAD_LENGTH = HEADER_LENGTH + INDEX_LENGTHS_SIZE;

    private final InternalArray.ElementGetter keyGetter;
    private final KeySerializer keySerializer;

    MapBlobElementSerializer(DataType keyType) {
        this.keyGetter = InternalArray.createElementGetter(keyType);
        this.keySerializer = createKeySerializer(keyType);
    }

    @Override
    public BlobElementSerializer.Writer createWriter(
            PositionOutputStream out,
            String blobFieldName,
            @Nullable BlobConsumer writeConsumer,
            boolean writeNullOnMissingFile,
            boolean writeNullOnFetchFailure,
            BlobFetchMetricReporter blobFetchMetricReporter,
            int copyBufferSize,
            BlobStagingFactory stagingFactory) {
        return new Writer(
                out,
                blobFieldName,
                writeConsumer,
                writeNullOnMissingFile,
                writeNullOnFetchFailure,
                blobFetchMetricReporter,
                copyBufferSize,
                stagingFactory,
                keyGetter,
                keySerializer);
    }

    @Override
    public boolean requiresReadInputStream(boolean blobAsDescriptor) {
        return true;
    }

    @Override
    public BlobElementSerializer.Reader createReader(
            FileIO fileIO,
            Path filePath,
            @Nullable SeekableInputStream in,
            boolean blobAsDescriptor) {
        return new Reader(fileIO, filePath, in, blobAsDescriptor, keySerializer);
    }

    /** Writer for a MAP&lt;X, BLOB&gt; field. */
    static final class Writer extends AbstractBlobElementWriter {

        private final InternalArray.ElementGetter keyGetter;
        private final KeySerializer keySerializer;

        Writer(
                PositionOutputStream out,
                String blobFieldName,
                @Nullable BlobConsumer writeConsumer,
                boolean writeNullOnMissingFile,
                boolean writeNullOnFetchFailure,
                BlobFetchMetricReporter blobFetchMetricReporter,
                int copyBufferSize,
                BlobStagingFactory stagingFactory,
                InternalArray.ElementGetter keyGetter,
                KeySerializer keySerializer) {
            super(
                    out,
                    blobFieldName,
                    writeConsumer,
                    writeNullOnMissingFile,
                    writeNullOnFetchFailure,
                    blobFetchMetricReporter,
                    copyBufferSize,
                    stagingFactory);
            this.keyGetter = keyGetter;
            this.keySerializer = keySerializer;
        }

        @Override
        public long write(InternalRow row) throws IOException {
            if (row.isNullAt(0)) {
                return writeNullElement();
            }

            InternalMap map = row.getMap(0);
            if (map == BlobMapPlaceholder.INSTANCE) {
                return writePlaceholderElement();
            }

            InternalArray keys = map.keyArray();
            InternalArray values = map.valueArray();
            if (keys.size() != map.size() || values.size() != map.size()) {
                throw new IllegalArgumentException(
                        "MAP<X, BLOB> key/value array size does not match map size.");
            }

            // 1. Serialize and validate the key array before writing the record.
            long[] keyLengths = new long[map.size()];
            byte[][] keyBytes = new byte[map.size()][];
            Set<ByteBuffer> seenKeys = new HashSet<>();
            for (int i = 0; i < map.size(); i++) {
                if (keys.isNullAt(i)) {
                    keyLengths[i] = NULL_KEY_LENGTH;
                    if (!seenKeys.add(null)) {
                        throw new IllegalArgumentException("MAP<X, BLOB> keys must be unique.");
                    }
                } else {
                    byte[] serializedKey =
                            keySerializer.serialize(keyGetter.getElementOrNull(keys, i));
                    keyLengths[i] = serializedKey.length;
                    keyBytes[i] = serializedKey;
                    if (!seenKeys.add(ByteBuffer.wrap(serializedKey))) {
                        throw new IllegalArgumentException("MAP<X, BLOB> keys must be unique.");
                    }
                }
            }

            long recordPosition = startRecord();
            // 2. Write meta
            write(MAGIC_NUMBER_BYTES);
            write(new byte[] {VERSION});
            write(intToLittleEndian(map.size()));

            // 3. Write key array
            for (byte[] serializedKey : keyBytes) {
                if (serializedKey != null) {
                    write(serializedKey);
                }
            }

            // 4. Write values(blobs) array, same as ArrayBlobElementWriter
            long[] valueLengths = new long[map.size()];
            boolean flush = false;
            for (int i = 0; i < map.size(); i++) {
                if (values.isNullAt(i)) {
                    valueLengths[i] = NULL_VALUE_LENGTH;
                    continue;
                }

                final int position = i;
                BlobFetchResult fetchResult = getBlob(() -> values.getBlob(position));
                Blob blob = fetchResult.blob();
                if (fetchResult.fetchFailure() || blob == null) {
                    valueLengths[i] = NULL_VALUE_LENGTH;
                    continue;
                }
                BlobCopySource source = prepareBlobSource(blob);
                if (source == null) {
                    valueLengths[i] = NULL_VALUE_LENGTH;
                    continue;
                }
                BlobCopySource prepared = prepareBlobForWrite(source);
                if (prepared == null) {
                    valueLengths[i] = NULL_VALUE_LENGTH;
                    continue;
                }
                final BlobDescriptor descriptor;
                try (BlobCopySource ignored = prepared) {
                    descriptor = writeBlobData(prepared);
                }
                valueLengths[i] = descriptor.length();
                flush |= accept(descriptor);
                recordSuccess(descriptor.length());
            }

            byte[] keyIndexBytes = DeltaVarintCompressor.compress(keyLengths);
            byte[] valueIndexBytes = DeltaVarintCompressor.compress(valueLengths);
            write(keyIndexBytes);
            write(valueIndexBytes);
            write(intToLittleEndian(keyIndexBytes.length));
            write(intToLittleEndian(valueIndexBytes.length));

            long recordLength = finishRecord(recordPosition);
            if (flush) {
                flush();
            }
            return recordLength;
        }
    }

    /** Reader for a MAP&lt;X, BLOB&gt; field. */
    static final class Reader extends AbstractBlobElementReader {

        private final KeySerializer keySerializer;

        Reader(
                FileIO fileIO,
                Path filePath,
                @Nullable SeekableInputStream in,
                boolean blobAsDescriptor,
                KeySerializer keySerializer) {
            super(fileIO, filePath, in, blobAsDescriptor);
            this.keySerializer = keySerializer;
        }

        @Override
        public Object read(long payloadPosition, long payloadLength) {
            SeekableInputStream in = inputStream();
            if (payloadPosition < 0
                    || payloadLength < MIN_PAYLOAD_LENGTH
                    || payloadPosition > Long.MAX_VALUE - payloadLength) {
                throw new IllegalArgumentException(
                        "Invalid MAP<X, BLOB> payload position or length: "
                                + payloadPosition
                                + ", "
                                + payloadLength);
            }

            try {
                byte[] header = new byte[HEADER_LENGTH];
                in.seek(payloadPosition);
                IOUtils.readFully(in, header);
                ByteBuffer headerBuffer = littleEndianBuffer(header);
                int magic = headerBuffer.getInt();
                if (magic != MAGIC_NUMBER) {
                    throw new IllegalArgumentException(
                            "Invalid MAP<X, BLOB> payload magic number: " + magic);
                }
                byte version = headerBuffer.get();
                if (version != VERSION) {
                    throw new UnsupportedOperationException(
                            "Unsupported MAP<X, BLOB> payload version: " + version);
                }
                int entryCount = headerBuffer.getInt();
                if (entryCount < 0) {
                    throw new IllegalArgumentException(
                            "Invalid MAP<X, BLOB> entry count: " + entryCount);
                }

                long payloadEnd = payloadPosition + payloadLength;
                long dataStart = payloadPosition + HEADER_LENGTH;
                long indexLengthsPosition = payloadEnd - INDEX_LENGTHS_SIZE;
                byte[] indexLengthBytes = new byte[INDEX_LENGTHS_SIZE];
                in.seek(indexLengthsPosition);
                IOUtils.readFully(in, indexLengthBytes);

                // key index lengths and value index lengths
                ByteBuffer indexLengthBuffer = littleEndianBuffer(indexLengthBytes);
                int keyIndexLength = indexLengthBuffer.getInt();
                int valueIndexLength = indexLengthBuffer.getInt();
                checkIndexLengths(keyIndexLength, valueIndexLength, payloadLength, entryCount);

                // 1. deserialize key indexes and value indexes
                long valueIndexStart = indexLengthsPosition - valueIndexLength;
                long keyIndexStart = valueIndexStart - keyIndexLength;

                byte[] keyIndexBytes = new byte[keyIndexLength];
                in.seek(keyIndexStart);
                IOUtils.readFully(in, keyIndexBytes);

                byte[] valueIndexBytes = new byte[valueIndexLength];
                in.seek(valueIndexStart);
                IOUtils.readFully(in, valueIndexBytes);

                long[] keyLengths;
                try {
                    keyLengths = DeltaVarintCompressor.decompress(keyIndexBytes);
                } catch (RuntimeException e) {
                    throw new IllegalArgumentException("Invalid MAP<X, BLOB> key index.", e);
                }

                long[] valueLengths;
                try {
                    valueLengths = DeltaVarintCompressor.decompress(valueIndexBytes);
                } catch (RuntimeException e) {
                    throw new IllegalArgumentException("Invalid MAP<X, BLOB> value index.", e);
                }
                long dataLength = keyIndexStart - dataStart;
                long keyDataLength = checkKeyLengths(keyLengths, dataLength, entryCount);
                checkBlobLengths(valueLengths, dataLength - keyDataLength, entryCount);

                // 2. deserialize keys
                Object[] keys = new Object[entryCount];
                long keyOffset = dataStart;
                for (int i = 0; i < entryCount; i++) {
                    long keyLength = keyLengths[i];
                    Object key;
                    if (keyLength == NULL_KEY_LENGTH) {
                        key = null;
                    } else {
                        byte[] keyBytes = new byte[(int) keyLength];
                        in.seek(keyOffset);
                        IOUtils.readFully(in, keyBytes);
                        try {
                            key = keySerializer.deserialize(keyBytes);
                        } catch (RuntimeException e) {
                            throw new IllegalArgumentException("Invalid MAP<X, BLOB> key.", e);
                        }
                        keyOffset += keyLength;
                    }
                    keys[i] = key;
                }

                // 3. deserialize values and construct map
                boolean binaryKey = keySerializer instanceof BinaryKeySerializer;
                Map<Object, Object> map = new LinkedHashMap<>();
                long valueOffset = dataStart + keyDataLength;
                for (int i = 0; i < entryCount; i++) {
                    long valueLength = valueLengths[i];
                    Object value =
                            valueLength == NULL_VALUE_LENGTH
                                    ? null
                                    : readBlob(valueOffset, valueLength);
                    if (valueLength != NULL_VALUE_LENGTH) {
                        valueOffset += valueLength;
                    }
                    map.put(keys[i], value);
                }
                GenericMap result =
                        binaryKey ? GenericMap.fromBinaryKeyMap(map) : new GenericMap(map);
                if (result.size() != entryCount) {
                    throw new IllegalArgumentException(
                            "Invalid MAP<X, BLOB> payload: duplicate key.");
                }
                return result;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Object placeholder() {
            return BlobMapPlaceholder.INSTANCE;
        }

        private void checkIndexLengths(
                int keyIndexLength, int valueIndexLength, long payloadLength, int entryCount) {
            long maximumIndexesLength = payloadLength - MIN_PAYLOAD_LENGTH;
            Preconditions.checkState(
                    keyIndexLength >= 0 && keyIndexLength <= maximumIndexesLength,
                    "Invalid MAP<X, BLOB> key index length: %s",
                    keyIndexLength);
            Preconditions.checkState(
                    valueIndexLength >= 0 && valueIndexLength <= maximumIndexesLength,
                    "Invalid MAP<X, BLOB> value index length: %s",
                    valueIndexLength);
            Preconditions.checkState(
                    (long) keyIndexLength + valueIndexLength <= maximumIndexesLength,
                    "MAP<X, BLOB> indexes exceed the payload length.");
            Preconditions.checkState(
                    entryCount <= keyIndexLength,
                    "MAP<X, BLOB> entry count exceeds key index length.");
            Preconditions.checkState(
                    entryCount <= valueIndexLength,
                    "MAP<X, BLOB> entry count exceeds value index length.");
        }

        private long checkKeyLengths(long[] keyLengths, long maxDataLength, int entryCount) {
            Preconditions.checkState(
                    keyLengths.length == entryCount,
                    "MAP<X, BLOB> entry count does not match key index length.");

            long keyDataLength = 0;
            int fixedKeyLength = keySerializer.fixedLength();
            for (long keyLength : keyLengths) {
                if (keyLength == NULL_KEY_LENGTH) {
                    continue;
                }
                Preconditions.checkState(
                        keyLength >= 0, "Invalid MAP<X, BLOB> key length: %s", keyLength);
                Preconditions.checkState(
                        keyLength <= Integer.MAX_VALUE,
                        "MAP<X, BLOB> key is too large: %s",
                        keyLength);
                Preconditions.checkState(
                        fixedKeyLength < 0 || keyLength == fixedKeyLength,
                        "Invalid MAP<X, BLOB> fixed-width key length: %s",
                        keyLength);
                Preconditions.checkState(
                        keyLength <= maxDataLength - keyDataLength,
                        "MAP<X, BLOB> key lengths exceed the payload data length.");
                keyDataLength += keyLength;
            }
            return keyDataLength;
        }

        private void checkBlobLengths(long[] blobLengths, long maxDataLength, int entryCount) {
            Preconditions.checkState(
                    blobLengths.length == entryCount,
                    "MAP<X, BLOB> entry count does not match value index length.");

            long valueDataLength = 0;
            for (long valueLength : blobLengths) {
                if (valueLength == NULL_VALUE_LENGTH) {
                    continue;
                }
                Preconditions.checkState(
                        valueLength >= 0, "Invalid MAP<X, BLOB> value length: %s", valueLength);
                Preconditions.checkState(
                        blobAsDescriptor() || valueLength <= Integer.MAX_VALUE,
                        "MAP<X, BLOB> inline value is too large: %s",
                        valueLength);
                Preconditions.checkState(
                        valueLength <= maxDataLength - valueDataLength,
                        "MAP<X, BLOB> value lengths exceed the payload data length.");
                valueDataLength += valueLength;
            }
            Preconditions.checkState(
                    valueDataLength == maxDataLength,
                    "MAP<X, BLOB> key/value lengths do not match the payload data length.");
        }
    }

    private static KeySerializer createKeySerializer(DataType keyType) {
        switch (keyType.getTypeRoot()) {
            case TINYINT:
                return new TinyIntKeySerializer();
            case SMALLINT:
                return new SmallIntKeySerializer();
            case INTEGER:
                return new IntKeySerializer();
            case BIGINT:
                return new BigIntKeySerializer();
            case BOOLEAN:
                return new BooleanKeySerializer();
            case DECIMAL:
                DecimalType decimalType = (DecimalType) keyType;
                return new DecimalKeySerializer(decimalType.getPrecision(), decimalType.getScale());
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return new IntKeySerializer();
            case BINARY:
            case VARBINARY:
                return new BinaryKeySerializer();
            case CHAR:
            case VARCHAR:
                return new StringKeySerializer();
            default:
                throw new IllegalArgumentException(
                        "Unsupported key type for MAP<X, BLOB>: " + keyType);
        }
    }

    private static void checkKeyLength(byte[] bytes, int expectedLength) {
        if (bytes.length != expectedLength) {
            throw new IllegalArgumentException(
                    "Expected " + expectedLength + " key bytes, but found " + bytes.length + '.');
        }
    }

    /** Key serializer for Map-Blob type. */
    private interface KeySerializer {

        byte[] serialize(Object key);

        Object deserialize(byte[] bytes);

        int fixedLength();
    }

    /** {@link KeySerializer} for Tiny Int Type. */
    private static final class TinyIntKeySerializer implements KeySerializer {

        @Override
        public byte[] serialize(Object key) {
            return new byte[] {(Byte) key};
        }

        @Override
        public Object deserialize(byte[] bytes) {
            checkKeyLength(bytes, Byte.BYTES);
            return bytes[0];
        }

        @Override
        public int fixedLength() {
            return Byte.BYTES;
        }
    }

    /** {@link KeySerializer} for Small Int Type. */
    private static final class SmallIntKeySerializer implements KeySerializer {

        @Override
        public byte[] serialize(Object key) {
            return littleEndianBuffer(new byte[Short.BYTES]).putShort((Short) key).array();
        }

        @Override
        public Object deserialize(byte[] bytes) {
            checkKeyLength(bytes, Short.BYTES);
            return littleEndianBuffer(bytes).getShort();
        }

        @Override
        public int fixedLength() {
            return Short.BYTES;
        }
    }

    /** {@link KeySerializer} for Int Type. */
    private static final class IntKeySerializer implements KeySerializer {

        @Override
        public byte[] serialize(Object key) {
            return intToLittleEndian((Integer) key);
        }

        @Override
        public Object deserialize(byte[] bytes) {
            checkKeyLength(bytes, Integer.BYTES);
            return littleEndianBuffer(bytes).getInt();
        }

        @Override
        public int fixedLength() {
            return Integer.BYTES;
        }
    }

    /** {@link KeySerializer} for Big Int Type. */
    private static final class BigIntKeySerializer implements KeySerializer {

        @Override
        public byte[] serialize(Object key) {
            return longToLittleEndian((Long) key);
        }

        @Override
        public Object deserialize(byte[] bytes) {
            checkKeyLength(bytes, Long.BYTES);
            return littleEndianBuffer(bytes).getLong();
        }

        @Override
        public int fixedLength() {
            return Long.BYTES;
        }
    }

    /** {@link KeySerializer} for Boolean Type. */
    private static final class BooleanKeySerializer implements KeySerializer {

        @Override
        public byte[] serialize(Object key) {
            return new byte[] {(Boolean) key ? (byte) 1 : (byte) 0};
        }

        @Override
        public Object deserialize(byte[] bytes) {
            checkKeyLength(bytes, Byte.BYTES);
            if (bytes[0] == 0) {
                return false;
            }
            if (bytes[0] == 1) {
                return true;
            }
            throw new IllegalArgumentException("Invalid MAP<X, BLOB> boolean key.");
        }

        @Override
        public int fixedLength() {
            return Byte.BYTES;
        }
    }

    /** {@link KeySerializer} for Decimal Type. */
    private static final class DecimalKeySerializer implements KeySerializer {

        private final int precision;
        private final int scale;

        private DecimalKeySerializer(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public byte[] serialize(Object key) {
            Decimal decimal = (Decimal) key;
            return Decimal.isCompact(precision)
                    ? longToLittleEndian(decimal.toUnscaledLong())
                    : decimal.toUnscaledBytes();
        }

        @Override
        public Object deserialize(byte[] bytes) {
            Decimal decimal;
            if (Decimal.isCompact(precision)) {
                checkKeyLength(bytes, Long.BYTES);
                decimal =
                        Decimal.fromUnscaledLong(
                                littleEndianBuffer(bytes).getLong(), precision, scale);
            } else {
                decimal = Decimal.fromUnscaledBytes(bytes, precision, scale);
            }
            if (decimal == null) {
                throw new IllegalArgumentException(
                        "MAP<X, BLOB> decimal key exceeds declared precision.");
            }
            return decimal;
        }

        @Override
        public int fixedLength() {
            return Decimal.isCompact(precision) ? Long.BYTES : -1;
        }
    }

    /** {@link KeySerializer} for Binary and VarBinary Types. */
    private static final class BinaryKeySerializer implements KeySerializer {

        @Override
        public byte[] serialize(Object key) {
            return (byte[]) key;
        }

        @Override
        public Object deserialize(byte[] bytes) {
            return bytes;
        }

        @Override
        public int fixedLength() {
            return -1;
        }
    }

    /** {@link KeySerializer} for String Type. */
    private static final class StringKeySerializer implements KeySerializer {

        @Override
        public byte[] serialize(Object key) {
            return ((BinaryString) key).toBytes();
        }

        @Override
        public Object deserialize(byte[] bytes) {
            return BinaryString.fromBytes(bytes);
        }

        @Override
        public int fixedLength() {
            return -1;
        }
    }

    private static ByteBuffer littleEndianBuffer(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }
}
