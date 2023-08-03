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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.sink.SequenceGenerator;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Projection;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.paimon.CoreOptions.FIELDS_PREFIX;
import static org.apache.paimon.utils.InternalRowUtils.createFieldGetters;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the partial record, update
 * non-null fields on merge.
 */
public class PartialUpdateMergeFunction implements MergeFunction<KeyValue> {

    public static final String SEQUENCE_GROUP = "sequence-group";

    private final InternalRow.FieldGetter[] getters;
    private final boolean ignoreDelete;
    private final Map<Integer, SequenceGenerator> fieldSequences;

    private InternalRow currentKey;
    private long latestSequenceNumber;
    private boolean isEmpty;
    private GenericRow row;
    private KeyValue reused;

    protected PartialUpdateMergeFunction(
            InternalRow.FieldGetter[] getters,
            boolean ignoreDelete,
            Map<Integer, SequenceGenerator> fieldSequences) {
        this.getters = getters;
        this.ignoreDelete = ignoreDelete;
        this.fieldSequences = fieldSequences;
    }

    @Override
    public void reset() {
        this.currentKey = null;
        this.row = new GenericRow(getters.length);
        this.isEmpty = true;
    }

    @Override
    public void add(KeyValue kv) {
        // refresh key object to avoid reference overwritten
        currentKey = kv.key();

        // ignore delete?
        if (kv.valueKind().isRetract()) {
            if (ignoreDelete) {
                return;
            }

            if (fieldSequences.size() > 1) {
                retractWithSequenceGroup(kv);
                return;
            }

            if (kv.valueKind() == RowKind.UPDATE_BEFORE) {
                throw new IllegalArgumentException(
                        "Partial update can not accept update_before records, it is a bug.");
            }

            String msg =
                    String.join(
                            "By default, Partial update can not accept delete records,"
                                    + " you can choose one of the following solutions:",
                            "1. Configure 'partial-update.ignore-delete' to ignore delete records.",
                            "2. Configure 'sequence-group' to retract partial columns.");

            throw new IllegalArgumentException(msg);
        }

        latestSequenceNumber = kv.sequenceNumber();
        isEmpty = false;
        if (fieldSequences.isEmpty()) {
            updateNonNullFields(kv);
        } else {
            updateWithSequenceGroup(kv);
        }
    }

    private void updateNonNullFields(KeyValue kv) {
        for (int i = 0; i < getters.length; i++) {
            Object field = getters[i].getFieldOrNull(kv.value());
            if (field != null) {
                row.setField(i, field);
            }
        }
    }

    private void updateWithSequenceGroup(KeyValue kv) {
        for (int i = 0; i < getters.length; i++) {
            Object field = getters[i].getFieldOrNull(kv.value());
            SequenceGenerator sequenceGen = fieldSequences.get(i);
            if (sequenceGen == null) {
                if (field != null) {
                    row.setField(i, field);
                }
            } else {
                Long currentSeq = sequenceGen.generateNullable(kv.value());
                if (currentSeq != null) {
                    Long previousSeq = sequenceGen.generateNullable(row);
                    if (previousSeq == null || currentSeq >= previousSeq) {
                        row.setField(i, field);
                    }
                }
            }
        }
    }

    private void retractWithSequenceGroup(KeyValue kv) {
        for (int i = 0; i < getters.length; i++) {
            SequenceGenerator sequenceGen = fieldSequences.get(i);
            if (sequenceGen != null) {
                Long currentSeq = sequenceGen.generateNullable(kv.value());
                if (currentSeq != null) {
                    Long previousSeq = sequenceGen.generateNullable(row);
                    if (previousSeq == null || currentSeq >= previousSeq) {
                        if (sequenceGen.index() == i) {
                            // update sequence field
                            row.setField(i, getters[i].getFieldOrNull(kv.value()));
                        } else {
                            // retract normal field
                            row.setField(i, null);
                        }
                    }
                }
            }
        }
    }

    @Override
    @Nullable
    public KeyValue getResult() {
        if (isEmpty) {
            return null;
        }

        if (reused == null) {
            reused = new KeyValue();
        }
        return reused.replace(currentKey, latestSequenceNumber, RowKind.INSERT, row);
    }

    public static MergeFunctionFactory<KeyValue> factory(Options options, RowType rowType) {
        return new Factory(options, rowType);
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        private final boolean ignoreDelete;
        private final List<DataType> tableTypes;
        private final Map<Integer, SequenceGenerator> fieldSequences;

        private Factory(Options options, RowType rowType) {
            this.ignoreDelete = options.get(CoreOptions.PARTIAL_UPDATE_IGNORE_DELETE);
            this.tableTypes = rowType.getFieldTypes();

            List<String> fieldNames = rowType.getFieldNames();
            this.fieldSequences = new HashMap<>();
            for (Map.Entry<String, String> entry : options.toMap().entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                if (k.startsWith(FIELDS_PREFIX) && k.endsWith(SEQUENCE_GROUP)) {
                    String sequenceFieldName =
                            k.substring(
                                    FIELDS_PREFIX.length() + 1,
                                    k.length() - SEQUENCE_GROUP.length() - 1);
                    SequenceGenerator sequenceGen =
                            new SequenceGenerator(sequenceFieldName, rowType);
                    Arrays.stream(v.split(","))
                            .map(fieldNames::indexOf)
                            .forEach(
                                    field -> {
                                        if (fieldSequences.containsKey(field)) {
                                            throw new IllegalArgumentException(
                                                    String.format(
                                                            "Field %s is defined repeatedly by multiple groups: %s",
                                                            fieldNames.get(field), k));
                                        }
                                        fieldSequences.put(field, sequenceGen);
                                    });

                    // add self
                    fieldSequences.put(sequenceGen.index(), sequenceGen);
                }
            }
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            if (projection != null) {
                Map<Integer, SequenceGenerator> projectedSequences = new HashMap<>();
                int[] projects = Projection.of(projection).toTopLevelIndexes();
                Map<Integer, Integer> indexMap = new HashMap<>();
                for (int i = 0; i < projects.length; i++) {
                    indexMap.put(projects[i], i);
                }
                fieldSequences.forEach(
                        (field, sequence) -> {
                            int newField = indexMap.getOrDefault(field, -1);
                            if (newField != -1) {
                                int newSequenceId = indexMap.getOrDefault(sequence.index(), -1);
                                if (newSequenceId == -1) {
                                    throw new RuntimeException(
                                            String.format(
                                                    "Can not find new sequence field for new field. new field index is %s",
                                                    newField));
                                } else {
                                    projectedSequences.put(
                                            newField,
                                            new SequenceGenerator(
                                                    newSequenceId, sequence.fieldType()));
                                }
                            }
                        });
                return new PartialUpdateMergeFunction(
                        createFieldGetters(Projection.of(projection).project(tableTypes)),
                        ignoreDelete,
                        projectedSequences);
            } else {
                return new PartialUpdateMergeFunction(
                        createFieldGetters(tableTypes), ignoreDelete, fieldSequences);
            }
        }

        @Override
        public AdjustedProjection adjustProjection(@Nullable int[][] projection) {
            if (fieldSequences.isEmpty()) {
                return new AdjustedProjection(projection, null);
            }

            if (projection == null) {
                return new AdjustedProjection(null, null);
            }
            LinkedHashSet<Integer> extraFields = new LinkedHashSet<>();
            int[] topProjects = Projection.of(projection).toTopLevelIndexes();
            Set<Integer> indexSet = Arrays.stream(topProjects).boxed().collect(Collectors.toSet());
            for (int index : topProjects) {
                SequenceGenerator generator = fieldSequences.get(index);
                if (generator != null && !indexSet.contains(generator.index())) {
                    extraFields.add(generator.index());
                }
            }

            int[] allProjects =
                    Stream.concat(Arrays.stream(topProjects).boxed(), extraFields.stream())
                            .mapToInt(Integer::intValue)
                            .toArray();

            int[][] pushdown = Projection.of(allProjects).toNestedIndexes();
            int[][] outer =
                    Projection.of(IntStream.range(0, topProjects.length).toArray())
                            .toNestedIndexes();
            return new AdjustedProjection(pushdown, outer);
        }
    }
}
