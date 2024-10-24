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

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * This column vector is used to adapt hive's LongColumnVector to Paimon's boolean, byte, short, int
 * and long ColumnVector.
 */
public class OrcLongColumnVector extends AbstractOrcColumnVector
        implements org.apache.paimon.data.columnar.LongColumnVector,
                org.apache.paimon.data.columnar.BooleanColumnVector,
                org.apache.paimon.data.columnar.ByteColumnVector,
                org.apache.paimon.data.columnar.ShortColumnVector,
                org.apache.paimon.data.columnar.IntColumnVector {

    private final LongColumnVector vector;

    public OrcLongColumnVector(LongColumnVector vector, VectorizedRowBatch orcBatch) {
        super(vector, orcBatch);
        this.vector = vector;
    }

    @Override
    public long getLong(int i) {
        i = rowMapper(i);
        return vector.vector[i];
    }

    @Override
    public boolean getBoolean(int i) {
        i = rowMapper(i);
        return vector.vector[i] == 1;
    }

    @Override
    public byte getByte(int i) {
        i = rowMapper(i);
        return (byte) vector.vector[i];
    }

    @Override
    public int getInt(int i) {
        i = rowMapper(i);
        return (int) vector.vector[i];
    }

    @Override
    public short getShort(int i) {
        i = rowMapper(i);
        return (short) vector.vector[i];
    }
}
