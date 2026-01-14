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

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateVisitor;
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

/** A {@link TableRead} wrapper that checks splits for authorization information. */
public class AuthAwareTableRead implements TableRead {

    private final TableRead wrapped;
    private final RowType outputRowType;

    public AuthAwareTableRead(TableRead wrapped, RowType outputRowType) {
        this.wrapped = wrapped;
        this.outputRowType = outputRowType;
    }

    @Override
    public TableRead withMetricRegistry(MetricRegistry registry) {
        return new AuthAwareTableRead(wrapped.withMetricRegistry(registry), outputRowType);
    }

    @Override
    public TableRead executeFilter() {
        return new AuthAwareTableRead(wrapped.executeFilter(), outputRowType);
    }

    @Override
    public TableRead withIOManager(IOManager ioManager) {
        return new AuthAwareTableRead(wrapped.withIOManager(ioManager), outputRowType);
    }

    @Override
    public RecordReader<InternalRow> createReader(Split split) throws IOException {
        if (split instanceof QueryAuthSplit) {
            TableQueryAuthResult authResult = ((QueryAuthSplit) split).authResult();
            if (authResult != null) {
                RecordReader<InternalRow> reader =
                        wrapped.createReader(((QueryAuthSplit) split).dataSplit());
                // Apply row-level filter if present
                Predicate rowFilter = authResult.rowFilter();
                if (rowFilter != null) {
                    Predicate remappedFilter = remapPredicateToOutputRow(outputRowType, rowFilter);
                    if (remappedFilter != null) {
                        reader = new FilterRecordReader(reader, remappedFilter);
                    }
                }

                // Apply column masking if present
                Map<String, Transform> columnMasking = authResult.columnMasking();
                if (columnMasking != null && !columnMasking.isEmpty()) {
                    MaskingApplier applier = new MaskingApplier(outputRowType, columnMasking);
                    reader = new MaskingRecordReader(reader, applier);
                }

                return reader;
            }
        }
        return wrapped.createReader(split);
    }

    private static class FilterRecordReader implements RecordReader<InternalRow> {

        private final RecordReader<InternalRow> wrapped;
        private final Predicate predicate;

        private FilterRecordReader(RecordReader<InternalRow> wrapped, Predicate predicate) {
            this.wrapped = wrapped;
            this.predicate = predicate;
        }

        @Nullable
        @Override
        public RecordIterator<InternalRow> readBatch() throws IOException {
            RecordIterator<InternalRow> batch = wrapped.readBatch();
            if (batch == null) {
                return null;
            }
            return new FilterRecordIterator(batch, predicate);
        }

        @Override
        public void close() throws IOException {
            wrapped.close();
        }
    }

    private static class FilterRecordIterator implements RecordReader.RecordIterator<InternalRow> {

        private final RecordReader.RecordIterator<InternalRow> wrapped;
        private final Predicate predicate;

        private FilterRecordIterator(
                RecordReader.RecordIterator<InternalRow> wrapped, Predicate predicate) {
            this.wrapped = wrapped;
            this.predicate = predicate;
        }

        @Nullable
        @Override
        public InternalRow next() throws IOException {
            while (true) {
                InternalRow row = wrapped.next();
                if (row == null) {
                    return null;
                }
                if (predicate.test(row)) {
                    return row;
                }
            }
        }

        @Override
        public void releaseBatch() {
            wrapped.releaseBatch();
        }
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

    private static Predicate remapPredicateToOutputRow(RowType outputRowType, Predicate predicate) {
        return predicate.visit(new PredicateRemapper(outputRowType));
    }

    private static class PredicateRemapper implements PredicateVisitor<Predicate> {
        private final RowType outputRowType;

        private PredicateRemapper(RowType outputRowType) {
            this.outputRowType = outputRowType;
        }

        @Override
        public Predicate visit(LeafPredicate predicate) {
            Transform transform = predicate.transform();
            List<Object> newInputs = new ArrayList<>();
            boolean hasUnmappedField = false;
            for (Object input : transform.inputs()) {
                if (input instanceof FieldRef) {
                    FieldRef ref = (FieldRef) input;
                    String fieldName = ref.name();
                    int newIndex = outputRowType.getFieldIndex(fieldName);
                    if (newIndex < 0) {
                        hasUnmappedField = true;
                        break;
                    }
                    DataType type = outputRowType.getTypeAt(newIndex);
                    newInputs.add(new FieldRef(newIndex, fieldName, type));
                } else {
                    newInputs.add(input);
                }
            }
            if (hasUnmappedField) {
                return null;
            }
            return predicate.copyWithNewInputs(newInputs);
        }

        @Override
        public Predicate visit(CompoundPredicate predicate) {
            List<Predicate> remappedChildren = new ArrayList<>();
            for (Predicate child : predicate.children()) {
                Predicate remapped = child.visit(this);
                if (remapped != null) {
                    remappedChildren.add(remapped);
                }
            }
            if (remappedChildren.isEmpty()) {
                return null;
            }
            if (remappedChildren.size() == 1) {
                return remappedChildren.get(0);
            }
            return new CompoundPredicate(predicate.function(), remappedChildren);
        }
    }
}
