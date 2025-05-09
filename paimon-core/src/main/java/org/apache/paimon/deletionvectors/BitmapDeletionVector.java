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

import org.apache.paimon.utils.RoaringBitmap32;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * A {@link DeletionVector} based on {@link RoaringBitmap32}, it only supports files with row count
 * not exceeding {@link RoaringBitmap32#MAX_VALUE}.
 */
public class BitmapDeletionVector implements DeletionVector {

    public static final int MAGIC_NUMBER = 1581511376;
    public static final int MAGIC_NUMBER_SIZE_BYTES = 4;

    private final RoaringBitmap32 roaringBitmap;

    public BitmapDeletionVector() {
        this.roaringBitmap = new RoaringBitmap32();
    }

    private BitmapDeletionVector(RoaringBitmap32 roaringBitmap) {
        this.roaringBitmap = roaringBitmap;
    }

    @Override
    public void delete(long position) {
        checkPosition(position);
        roaringBitmap.add((int) position);
    }

    @Override
    public void merge(DeletionVector deletionVector) {
        if (deletionVector instanceof BitmapDeletionVector) {
            roaringBitmap.or(((BitmapDeletionVector) deletionVector).roaringBitmap);
        } else {
            throw new RuntimeException("Only instance with the same class type can be merged.");
        }
    }

    @Override
    public boolean checkedDelete(long position) {
        checkPosition(position);
        return roaringBitmap.checkedAdd((int) position);
    }

    @Override
    public boolean isDeleted(long position) {
        checkPosition(position);
        return roaringBitmap.contains((int) position);
    }

    @Override
    public boolean isEmpty() {
        return roaringBitmap.isEmpty();
    }

    @Override
    public long getCardinality() {
        return roaringBitmap.getCardinality();
    }

    @Override
    public int serializeTo(DataOutputStream out) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(MAGIC_NUMBER);
            roaringBitmap.serialize(dos);
            byte[] data = bos.toByteArray();
            int size = data.length;
            out.writeInt(size);
            out.write(data);
            out.writeInt(calculateChecksum(data));
            return size;
        } catch (Exception e) {
            throw new RuntimeException("Unable to serialize deletion vector", e);
        }
    }

    /**
     * Note: the result is read only, do not call any modify operation outside.
     *
     * @return the deleted position
     */
    public RoaringBitmap32 get() {
        return roaringBitmap;
    }

    public static DeletionVector deserializeFromByteBuffer(ByteBuffer buffer) throws IOException {
        RoaringBitmap32 bitmap = new RoaringBitmap32();
        bitmap.deserialize(buffer);
        return new BitmapDeletionVector(bitmap);
    }

    private void checkPosition(long position) {
        if (position > RoaringBitmap32.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "The file has too many rows, RoaringBitmap32 only supports files with row count not exceeding 2147483647.");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BitmapDeletionVector that = (BitmapDeletionVector) o;
        return Objects.equals(this.roaringBitmap, that.roaringBitmap);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(roaringBitmap);
    }

    public static int calculateChecksum(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }
}
