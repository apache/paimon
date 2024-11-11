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

package org.apache.paimon.hash;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.types.DataType;

/** Test case for {@link BytesHashMap}. */
public class BytesHashMapTest extends BytesHashMapTestBase<BinaryRow> {

    public BytesHashMapTest() {
        super(new BinaryRowSerializer(KEY_TYPES.length));
    }

    @Override
    public BytesHashMap<BinaryRow> createBytesHashMap(
            MemorySegmentPool pool, DataType[] keyTypes, DataType[] valueTypes) {
        return new BytesHashMap<>(
                pool, new BinaryRowSerializer(keyTypes.length), valueTypes.length);
    }

    @Override
    public BinaryRow[] generateRandomKeys(int num) {
        return getRandomizedInputs(num);
    }
}
