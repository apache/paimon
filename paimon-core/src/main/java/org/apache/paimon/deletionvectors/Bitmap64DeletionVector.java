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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.utils.OptimizedRoaringBitmap64;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RoaringBitmap32;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * A {@link DeletionVector} based on {@link OptimizedRoaringBitmap64}, it only supports files with
 * row count not exceeding {@link OptimizedRoaringBitmap64#MAX_VALUE}.
 *
 * <p>Mostly copied from iceberg.
 */
public class Bitmap64DeletionVector implements DeletionVector {

    public static final int VERSION = 2;

    public static final int MAGIC_NUMBER = 1681511377;
    public static final int LENGTH_SIZE_BYTES = 4;
    public static final int CRC_SIZE_BYTES = 4;
    private static final int MAGIC_NUMBER_SIZE_BYTES = 4;
    private static final int BITMAP_DATA_OFFSET = 4;

    private final OptimizedRoaringBitmap64 roaringBitmap;

    public Bitmap64DeletionVector() {
        this.roaringBitmap = new OptimizedRoaringBitmap64();
    }

    private Bitmap64DeletionVector(OptimizedRoaringBitmap64 roaringBitmap) {
        this.roaringBitmap = roaringBitmap;
    }

    public static Bitmap64DeletionVector fromBitmapDeletionVector(
            BitmapDeletionVector bitmapDeletionVector) {
        RoaringBitmap32 roaringBitmap32 = bitmapDeletionVector.get();
        return new Bitmap64DeletionVector(
                OptimizedRoaringBitmap64.fromRoaringBitmap32(roaringBitmap32));
    }

    @Override
    public void delete(long position) {
        roaringBitmap.add(position);
    }

    @Override
    public void merge(DeletionVector deletionVector) {
        if (deletionVector instanceof Bitmap64DeletionVector) {
            roaringBitmap.or(((Bitmap64DeletionVector) deletionVector).roaringBitmap);
        } else {
            throw new RuntimeException("Only instance with the same class type can be merged.");
        }
    }

    @Override
    public boolean isDeleted(long position) {
        return roaringBitmap.contains(position);
    }

    @Override
    public boolean isEmpty() {
        return roaringBitmap.isEmpty();
    }

    @Override
    public long getCardinality() {
        return roaringBitmap.cardinality();
    }

    @Override
    public int dvVersion() {
        return VERSION;
    }

    @Override
    public byte[] serializeToBytes() {
        roaringBitmap.runLengthEncode(); // run-length encode the bitmap before serializing
        int bitmapDataLength = computeBitmapDataLength(roaringBitmap); // magic bytes + bitmap
        byte[] bytes = new byte[LENGTH_SIZE_BYTES + bitmapDataLength + CRC_SIZE_BYTES];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putInt(bitmapDataLength);
        serializeBitmapData(bytes, bitmapDataLength, roaringBitmap);
        int crcOffset = LENGTH_SIZE_BYTES + bitmapDataLength;
        int crc = computeChecksum(bytes, bitmapDataLength);
        buffer.putInt(crcOffset, crc);
        buffer.rewind();
        return bytes;
    }

    public static DeletionVector deserializeFromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int bitmapDataLength = readBitmapDataLength(buffer, bytes.length);
        OptimizedRoaringBitmap64 bitmap = deserializeBitmap(bytes, bitmapDataLength);
        int crc = computeChecksum(bytes, bitmapDataLength);
        int crcOffset = LENGTH_SIZE_BYTES + bitmapDataLength;
        int expectedCrc = buffer.getInt(crcOffset);
        Preconditions.checkArgument(crc == expectedCrc, "Invalid CRC");
        return new Bitmap64DeletionVector(bitmap);
    }

    // computes and validates the length of the bitmap data (magic bytes + bitmap)
    private static int computeBitmapDataLength(OptimizedRoaringBitmap64 bitmap) {
        long length = MAGIC_NUMBER_SIZE_BYTES + bitmap.serializedSizeInBytes();
        long bufferSize = LENGTH_SIZE_BYTES + length + CRC_SIZE_BYTES;
        Preconditions.checkState(bufferSize <= Integer.MAX_VALUE, "Can't serialize index > 2GB");
        return (int) length;
    }

    private static void serializeBitmapData(
            byte[] bytes, int bitmapDataLength, OptimizedRoaringBitmap64 bitmap) {
        ByteBuffer bitmapData = pointToBitmapData(bytes, bitmapDataLength);
        bitmapData.putInt(MAGIC_NUMBER);
        bitmap.serialize(bitmapData);
    }

    // points to the bitmap data in the blob
    private static ByteBuffer pointToBitmapData(byte[] bytes, int bitmapDataLength) {
        ByteBuffer bitmapData = ByteBuffer.wrap(bytes, BITMAP_DATA_OFFSET, bitmapDataLength);
        bitmapData.order(ByteOrder.LITTLE_ENDIAN);
        return bitmapData;
    }

    // checks the size is equal to the bitmap data length + extra bytes for length and CRC
    private static int readBitmapDataLength(ByteBuffer buffer, int size) {
        int length = buffer.getInt();
        int expectedLength = size - LENGTH_SIZE_BYTES - CRC_SIZE_BYTES;
        Preconditions.checkArgument(
                length == expectedLength,
                "Invalid bitmap data length: %s, expected %s",
                length,
                expectedLength);
        return length;
    }

    // validates magic bytes and deserializes the bitmap
    private static OptimizedRoaringBitmap64 deserializeBitmap(byte[] bytes, int bitmapDataLength) {
        ByteBuffer bitmapData = pointToBitmapData(bytes, bitmapDataLength);
        int magicNumber = bitmapData.getInt();
        Preconditions.checkArgument(
                magicNumber == MAGIC_NUMBER,
                "Invalid magic number: %s, expected %s",
                magicNumber,
                MAGIC_NUMBER);
        return OptimizedRoaringBitmap64.deserialize(bitmapData);
    }

    // generates a 32-bit unsigned checksum for the magic bytes and serialized bitmap
    private static int computeChecksum(byte[] bytes, int bitmapDataLength) {
        CRC32 crc = new CRC32();
        crc.update(bytes, BITMAP_DATA_OFFSET, bitmapDataLength);
        return (int) crc.getValue();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Bitmap64DeletionVector that = (Bitmap64DeletionVector) o;
        return Objects.equals(this.roaringBitmap, that.roaringBitmap);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(roaringBitmap);
    }
}
