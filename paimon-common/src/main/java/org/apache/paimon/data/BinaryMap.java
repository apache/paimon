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

package org.apache.paimon.data;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.types.DataType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * [4 byte(keyArray size in bytes)] + [Key BinaryArray] + [Value BinaryArray].
 *
 * <p>{@code BinaryMap} are influenced by Apache Spark UnsafeMapData.
 *
 * @since 0.4.0
 */
@Public
public final class BinaryMap extends BinarySection implements InternalMap {

    private static final long serialVersionUID = 1L;

    private transient BinaryArray keys;
    private transient BinaryArray values;

    public BinaryMap() {
        keys = new BinaryArray();
        values = new BinaryArray();
    }

    public int size() {
        return keys.size();
    }

    @Override
    public void pointTo(MemorySegment[] segments, int offset, int sizeInBytes) {
        // Read the numBytes of key array from the first 4 bytes.
        final int keyArrayBytes = MemorySegmentUtils.getInt(segments, offset);
        assert keyArrayBytes >= 0 : "keyArraySize (" + keyArrayBytes + ") should >= 0";
        final int valueArrayBytes = sizeInBytes - keyArrayBytes - 4;
        assert valueArrayBytes >= 0 : "valueArraySize (" + valueArrayBytes + ") should >= 0";

        // see BinarySection.readObject, on this call stack, keys and values are not initialized
        if (keys == null) {
            keys = new BinaryArray();
        }
        keys.pointTo(segments, offset + 4, keyArrayBytes);
        if (values == null) {
            values = new BinaryArray();
        }
        values.pointTo(segments, offset + 4 + keyArrayBytes, valueArrayBytes);

        assert keys.size() == values.size();

        this.segments = segments;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
    }

    public BinaryArray keyArray() {
        return keys;
    }

    public BinaryArray valueArray() {
        return values;
    }

    public Map<?, ?> toJavaMap(DataType keyType, DataType valueType) {
        Object[] keyArray = keys.toObjectArray(keyType);
        Object[] valueArray = values.toObjectArray(valueType);

        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < keyArray.length; i++) {
            map.put(keyArray[i], valueArray[i]);
        }
        return map;
    }

    public BinaryMap copy() {
        return copy(new BinaryMap());
    }

    public BinaryMap copy(BinaryMap reuse) {
        byte[] bytes = MemorySegmentUtils.copyToBytes(segments, offset, sizeInBytes);
        reuse.pointTo(MemorySegment.wrap(bytes), 0, sizeInBytes);
        return reuse;
    }

    @Override
    public int hashCode() {
        return MemorySegmentUtils.hashByWords(segments, offset, sizeInBytes);
    }

    // ------------------------------------------------------------------------------------------
    // Construction Utilities
    // ------------------------------------------------------------------------------------------

    public static BinaryMap valueOf(BinaryArray key, BinaryArray value) {
        checkArgument(key.segments.length == 1 && value.getSegments().length == 1);
        byte[] bytes = new byte[4 + key.sizeInBytes + value.sizeInBytes];
        MemorySegment segment = MemorySegment.wrap(bytes);
        segment.putInt(0, key.sizeInBytes);
        key.getSegments()[0].copyTo(key.getOffset(), segment, 4, key.sizeInBytes);
        value.getSegments()[0].copyTo(
                value.getOffset(), segment, 4 + key.sizeInBytes, value.sizeInBytes);
        BinaryMap map = new BinaryMap();
        map.pointTo(segment, 0, bytes.length);
        return map;
    }
}
