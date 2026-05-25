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

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.types.RowType;

import java.util.List;

/**
 * A {@link ChannelComputer} that partitions records by a compound hash of partition fields and
 * upsert key columns. Uses {@code indexParallelism} to control how many subtasks each partition can
 * be distributed to:
 *
 * <pre>
 * slot = abs(partitionHash) * indexParallelism + abs(keyHash) % indexParallelism
 * channel = slot % numChannels
 * </pre>
 *
 * <p>With {@code indexParallelism=1}, each partition maps to exactly one subtask, so the upsert key
 * index is loaded only once per partition. Higher values spread the load across more subtasks at
 * the cost of redundant index loading.
 */
public class UpsertKeyChannelComputer implements ChannelComputer<InternalRow> {

    private static final long serialVersionUID = 1L;

    private final TableSchema schema;
    private final List<String> upsertKeyColumns;
    private final int indexParallelism;

    private transient int numChannels;
    private transient FieldGetter[] keyFieldGetters;
    private transient FieldGetter[] partitionFieldGetters;

    public UpsertKeyChannelComputer(
            TableSchema schema, List<String> upsertKeyColumns, int indexParallelism) {
        this.schema = schema;
        this.upsertKeyColumns = upsertKeyColumns;
        this.indexParallelism = indexParallelism;
    }

    @Override
    public void setup(int numChannels) {
        this.numChannels = numChannels;
        RowType rowType = schema.logicalRowType();

        this.keyFieldGetters = new FieldGetter[upsertKeyColumns.size()];
        for (int i = 0; i < upsertKeyColumns.size(); i++) {
            int idx = rowType.getFieldIndex(upsertKeyColumns.get(i));
            keyFieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(idx), idx);
        }

        List<String> partitionKeys = schema.partitionKeys();
        this.partitionFieldGetters = new FieldGetter[partitionKeys.size()];
        for (int i = 0; i < partitionKeys.size(); i++) {
            int idx = rowType.getFieldIndex(partitionKeys.get(i));
            partitionFieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(idx), idx);
        }
    }

    @Override
    public int channel(InternalRow record) {
        int partHash = 0;
        for (FieldGetter getter : partitionFieldGetters) {
            Object val = getter.getFieldOrNull(record);
            partHash = partHash * 31 + (val == null ? 0 : val.hashCode());
        }

        int keyHash = 0;
        for (FieldGetter getter : keyFieldGetters) {
            Object val = getter.getFieldOrNull(record);
            keyHash = keyHash * 31 + (val == null ? 0 : val.hashCode());
        }

        long slot =
                (long) (partHash & Integer.MAX_VALUE) * indexParallelism
                        + (keyHash & Integer.MAX_VALUE) % indexParallelism;
        return (int) (slot % numChannels);
    }

    @Override
    public String toString() {
        return "shuffle by partition+upsert key "
                + upsertKeyColumns
                + " (indexParallelism="
                + indexParallelism
                + ")";
    }
}
