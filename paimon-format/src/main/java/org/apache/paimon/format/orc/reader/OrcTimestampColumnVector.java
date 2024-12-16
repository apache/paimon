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

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.utils.DateTimeUtils;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * This column vector is used to adapt hive's TimestampColumnVector to Paimon's
 * TimestampColumnVector.
 */
public class OrcTimestampColumnVector extends AbstractOrcColumnVector
        implements org.apache.paimon.data.columnar.TimestampColumnVector {

    private final TimestampColumnVector vector;

    public OrcTimestampColumnVector(ColumnVector vector, VectorizedRowBatch orcBatch) {
        super(vector, orcBatch);
        this.vector = (TimestampColumnVector) vector;
    }

    @Override
    public Timestamp getTimestamp(int i, int precision) {
        i = rowMapper(i);
        return DateTimeUtils.toInternal(vector.time[i], vector.nanos[i] % 1_000_000);
    }
}
