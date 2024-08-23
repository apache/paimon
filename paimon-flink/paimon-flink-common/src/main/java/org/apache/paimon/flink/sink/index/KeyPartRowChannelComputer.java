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

package org.apache.paimon.flink.sink.index;

import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.crosspartition.KeyPartOrRow;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

/** A {@link ChannelComputer} for KeyPartOrRow and row. */
public class KeyPartRowChannelComputer
        implements ChannelComputer<Tuple2<KeyPartOrRow, InternalRow>> {

    private static final long serialVersionUID = 1L;

    private final RowType rowType;
    private final RowType keyPartType;
    private final List<String> primaryKey;

    private transient int numChannels;
    private transient Projection rowProject;
    private transient Projection keyPartProject;

    public KeyPartRowChannelComputer(
            RowType rowType, RowType keyPartType, List<String> primaryKey) {
        this.rowType = rowType;
        this.keyPartType = keyPartType;
        this.primaryKey = primaryKey;
    }

    @Override
    public void setup(int numChannels) {
        this.numChannels = numChannels;
        this.rowProject = CodeGenUtils.newProjection(rowType, primaryKey);
        this.keyPartProject = CodeGenUtils.newProjection(keyPartType, primaryKey);
    }

    @Override
    public int channel(Tuple2<KeyPartOrRow, InternalRow> record) {
        BinaryRow key =
                (record.f0 == KeyPartOrRow.KEY_PART ? keyPartProject : rowProject).apply(record.f1);
        return Math.abs(key.hashCode() % numChannels);
    }

    @Override
    public String toString() {
        return "shuffle by key hash";
    }
}
