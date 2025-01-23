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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.columnar.ArrayColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarArray;

/** This class represents a nullable heap array column vector. */
public class HeapArrayVector extends AbstractArrayBasedVector implements ArrayColumnVector {

    public HeapArrayVector(int len, ColumnVector vector) {
        super(len, new ColumnVector[] {vector});
    }

    public void setChild(ColumnVector child) {
        children[0] = child;
    }

    @Override
    public InternalArray getArray(int i) {
        long offset = offsets[i];
        long length = lengths[i];
        return new ColumnarArray(children[0], (int) offset, (int) length);
    }

    @Override
    public ColumnVector getColumnVector() {
        return children[0];
    }
}
