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

package org.apache.paimon.flink;

import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobView;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.MapData;

import java.util.Set;

/** Convert to Flink row data with blob. */
public class FlinkRowDataWithBlob extends FlinkRowData {

    private final Set<Integer> blobFields;
    private final boolean blobAsDescriptor;

    public FlinkRowDataWithBlob(
            InternalRow row, Set<Integer> blobFields, boolean blobAsDescriptor) {
        super(row);
        this.blobFields = blobFields;
        this.blobAsDescriptor = blobAsDescriptor;
    }

    @Override
    public byte[] getBinary(int pos) {
        if (blobFields.contains(pos)) {
            return blobToBytes(row.getBlob(pos));
        } else {
            return row.getBinary(pos);
        }
    }

    @Override
    public ArrayData getArray(int pos) {
        if (blobFields.contains(pos)) {
            InternalArray array = row.getArray(pos);
            return toFlinkBlobArray(array);
        } else {
            return super.getArray(pos);
        }
    }

    @Override
    public MapData getMap(int pos) {
        if (!blobFields.contains(pos)) {
            return super.getMap(pos);
        }

        InternalMap map = row.getMap(pos);
        InternalArray keys = map.keyArray();

        // Keys should never be null in FlinkMap, but Paimon could. Check here.
        for (int i = 0; i < map.size(); i++) {
            if (keys.isNullAt(i)) {
                throw new IllegalArgumentException(
                        "Flink MAP<X, BLOB> does not support null keys.");
            }
        }

        return new MapData() {
            @Override
            public int size() {
                return map.size();
            }

            @Override
            public ArrayData keyArray() {
                return new FlinkArrayData(keys);
            }

            @Override
            public ArrayData valueArray() {
                return toFlinkBlobArray(map.valueArray());
            }
        };
    }

    private ArrayData toFlinkBlobArray(InternalArray array) {
        Object[] elements = new Object[array.size()];

        for (int i = 0; i < array.size(); i++) {
            elements[i] = array.isNullAt(i) ? null : blobToBytes(array.getBlob(i));
        }

        return new GenericArrayData(elements);
    }

    private byte[] blobToBytes(Blob blob) {
        if (blob == null) {
            return null;
        }
        if (blob instanceof BlobView && !((BlobView) blob).isResolved()) {
            return Blob.serializeBlob(blob);
        }
        return blobAsDescriptor ? blob.toDescriptor().serialize() : blob.toData();
    }
}
