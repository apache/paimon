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

import org.apache.paimon.data.Decimal;

import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.math.BigDecimal;

/**
 * This column vector is used to adapt hive's DecimalColumnVector to Paimon's DecimalColumnVector.
 */
public class OrcDecimalColumnVector extends AbstractOrcColumnVector
        implements org.apache.paimon.data.columnar.DecimalColumnVector {

    private final DecimalColumnVector vector;

    public OrcDecimalColumnVector(DecimalColumnVector vector, VectorizedRowBatch orcBatch) {
        super(vector, orcBatch);
        this.vector = vector;
    }

    @Override
    public Decimal getDecimal(int i, int precision, int scale) {
        i = rowMapper(i);
        BigDecimal data = vector.vector[i].getHiveDecimal().bigDecimalValue();
        return Decimal.fromBigDecimal(data, precision, scale);
    }
}
