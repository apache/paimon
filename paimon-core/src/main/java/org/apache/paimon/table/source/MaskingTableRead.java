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

package org.apache.paimon.table.source;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.InternalRowUtils.get;

/**
 * A {@link TableRead} wrapper which applies column masking for each produced {@link InternalRow}.
 */
public class MaskingTableRead implements TableRead {

    private final TableRead wrapped;
    private final RowType outputRowType;
    private final Map<String, Transform> masking;
    private final MaskingApplier applier;

    public MaskingTableRead(
            TableRead wrapped, RowType outputRowType, Map<String, Transform> masking) {
        this.wrapped = wrapped;
        this.outputRowType = outputRowType;
        this.masking = masking;
        this.applier = new MaskingApplier(outputRowType, masking);
    }

    @Override
    public TableRead withMetricRegistry(MetricRegistry registry) {
        return new MaskingTableRead(wrapped.withMetricRegistry(registry), outputRowType, masking);
    }

    @Override
    public TableRead executeFilter() {
        return new MaskingTableRead(wrapped.executeFilter(), outputRowType, masking);
    }

    @Override
    public TableRead withIOManager(IOManager ioManager) {
        return new MaskingTableRead(wrapped.withIOManager(ioManager), outputRowType, masking);
    }

    @Override
    public RecordReader<InternalRow> createReader(Split split) throws IOException {
        return new MaskingRecordReader(wrapped.createReader(split), applier);
    }

    private static class MaskingRecordReader implements RecordReader<InternalRow> {

        private final RecordReader<InternalRow> wrapped;
        private final MaskingApplier applier;

        private MaskingRecordReader(RecordReader<InternalRow> wrapped, MaskingApplier applier) {
            this.wrapped = wrapped;
            this.applier = applier;
        }

        @Nullable
        @Override
        public RecordIterator<InternalRow> readBatch() throws IOException {
            RecordIterator<InternalRow> batch = wrapped.readBatch();
            if (batch == null) {
                return null;
            }
            return batch.transform(applier::apply);
        }

        @Override
        public void close() throws IOException {
            wrapped.close();
        }
    }

    private static class MaskingApplier {

        private final RowType outputRowType;
        private final Map<Integer, Transform> remapped;

        private MaskingApplier(RowType outputRowType, Map<String, Transform> masking) {
            this.outputRowType = outputRowType;
            this.remapped = remapToOutputRow(outputRowType, masking);
        }

        private InternalRow apply(InternalRow row) {
            if (remapped.isEmpty()) {
                return row;
            }
            int arity = outputRowType.getFieldCount();
            GenericRow out = new GenericRow(row.getRowKind(), arity);
            for (int i = 0; i < arity; i++) {
                DataType type = outputRowType.getTypeAt(i);
                out.setField(i, get(row, i, type));
            }
            for (Map.Entry<Integer, Transform> e : remapped.entrySet()) {
                int targetIndex = e.getKey();
                Transform transform = e.getValue();
                Object masked = transform.transform(row);
                out.setField(targetIndex, masked);
            }
            return out;
        }

        private static Map<Integer, Transform> remapToOutputRow(
                RowType outputRowType, Map<String, Transform> masking) {
            Map<Integer, Transform> out = new HashMap<>();
            if (masking == null || masking.isEmpty()) {
                return out;
            }

            for (Map.Entry<String, Transform> e : masking.entrySet()) {
                String targetColumn = e.getKey();
                Transform transform = e.getValue();
                if (targetColumn == null || transform == null) {
                    continue;
                }

                int targetIndex = outputRowType.getFieldIndex(targetColumn);
                if (targetIndex < 0) {
                    continue;
                }

                List<Object> newInputs = new ArrayList<>();
                for (Object input : transform.inputs()) {
                    if (input instanceof FieldRef) {
                        FieldRef ref = (FieldRef) input;
                        int newIndex = outputRowType.getFieldIndex(ref.name());
                        if (newIndex < 0) {
                            throw new IllegalArgumentException(
                                    "Column masking refers to field '"
                                            + ref.name()
                                            + "' which is not present in output row type "
                                            + outputRowType);
                        }
                        DataType type = outputRowType.getTypeAt(newIndex);
                        newInputs.add(new FieldRef(newIndex, ref.name(), type));
                    } else {
                        newInputs.add(input);
                    }
                }
                out.put(targetIndex, transform.copyWithNewInputs(newInputs));
            }
            return out;
        }
    }
}
