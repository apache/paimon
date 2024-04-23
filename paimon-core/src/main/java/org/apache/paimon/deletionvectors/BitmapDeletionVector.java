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
import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A {@link DeletionVector} based on {@link RoaringBitmap32}, it only supports files with row count
 * not exceeding {@link RoaringBitmap32#MAX_VALUE}.
 */
public class BitmapDeletionVector implements DeletionVector {

    public static final int MAGIC_NUMBER = 1581511376;

    private final RoaringBitmap32 roaringBitmap;

    BitmapDeletionVector() {
        roaringBitmap = new RoaringBitmap32();
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
    public byte[] serializeToBytes() {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(MAGIC_NUMBER);
            roaringBitmap.serialize(dos);
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Unable to serialize deletion vector", e);
        }
    }

    public static DeletionVector deserializeFromDataInput(DataInput bis) throws IOException {
        RoaringBitmap32 roaringBitmap = new RoaringBitmap32();
        roaringBitmap.deserialize(bis);
        return new BitmapDeletionVector(roaringBitmap);
    }

    private void checkPosition(long position) {
        if (position > RoaringBitmap32.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "The file has too many rows, RoaringBitmap32 only supports files with row count not exceeding 2147483647.");
        }
    }
}
