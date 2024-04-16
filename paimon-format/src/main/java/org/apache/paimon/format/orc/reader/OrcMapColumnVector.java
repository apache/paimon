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

package org.apache.paimon.format.orc.reader;

import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarMap;
import org.apache.paimon.types.MapType;

import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;

/** This column vector is used to adapt hive's MapColumnVector to Paimon's MapColumnVector. */
public class OrcMapColumnVector extends AbstractOrcColumnVector
        implements org.apache.paimon.data.columnar.MapColumnVector {

    private final MapColumnVector hiveVector;
    private final ColumnVector keyPaimonVector;
    private final ColumnVector valuePaimonVector;

    public OrcMapColumnVector(MapColumnVector hiveVector, MapType type) {
        super(hiveVector);
        this.hiveVector = hiveVector;
        this.keyPaimonVector = createPaimonVector(hiveVector.keys, type.getKeyType());
        this.valuePaimonVector = createPaimonVector(hiveVector.values, type.getValueType());
    }

    @Override
    public InternalMap getMap(int i) {
        long offset = hiveVector.offsets[i];
        long length = hiveVector.lengths[i];
        return new ColumnarMap(keyPaimonVector, valuePaimonVector, (int) offset, (int) length);
    }

    @Override
    public ColumnVector getKeyColumnVector() {
        return keyPaimonVector;
    }

    @Override
    public ColumnVector getValueColumnVector() {
        return valuePaimonVector;
    }
}
