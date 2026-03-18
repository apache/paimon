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

package org.apache.paimon.data.columnar.heap;

import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarMap;
import org.apache.paimon.data.columnar.MapColumnVector;

/** This class represents a nullable heap map column vector. */
public class HeapMapVector extends AbstractArrayBasedVector implements MapColumnVector {

    public HeapMapVector(int capacity, ColumnVector keys, ColumnVector values) {
        super(capacity, new ColumnVector[] {keys, values});
    }

    public void setKeys(ColumnVector keys) {
        children[0] = keys;
    }

    public ColumnVector getKeys() {
        return children[0];
    }

    public void setValues(ColumnVector values) {
        children[1] = values;
    }

    public ColumnVector getValues() {
        return children[1];
    }

    @Override
    public InternalMap getMap(int i) {
        long offset = offsets[i];
        long length = lengths[i];
        return new ColumnarMap(children[0], children[1], (int) offset, (int) length);
    }
}
