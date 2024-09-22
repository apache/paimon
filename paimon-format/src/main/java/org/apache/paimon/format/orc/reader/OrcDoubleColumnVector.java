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

import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;

/**
 * This column vector is used to adapt hive's DoubleColumnVector to Paimon's float and double
 * ColumnVector.
 */
public class OrcDoubleColumnVector extends AbstractOrcColumnVector
        implements org.apache.paimon.data.columnar.DoubleColumnVector,
                org.apache.paimon.data.columnar.FloatColumnVector {

    private final DoubleColumnVector vector;

    public OrcDoubleColumnVector(DoubleColumnVector vector, int[] selected) {
        super(vector, selected);
        this.vector = vector;
    }

    @Override
    public double getDouble(int i) {
        i = rowMapper(i);
        return vector.vector[vector.isRepeating ? 0 : i];
    }

    @Override
    public float getFloat(int i) {
        i = rowMapper(i);
        return (float) vector.vector[vector.isRepeating ? 0 : i];
    }
}
