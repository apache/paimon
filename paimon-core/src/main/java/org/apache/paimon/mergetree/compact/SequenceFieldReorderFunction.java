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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.table.sink.SequenceGenerator;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Projection;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Combine the Sequence Field defined in the Schema with the auto-incremented Sequence Number in
 * KeyValue, and reorder KeyValue with the same primary key.
 */
public class SequenceFieldReorderFunction implements ReorderFunction<KeyValue> {

    private final InternalRowSerializer keySerializer;
    private final InternalRowSerializer valueSerializer;
    private final SequenceGenerator sequenceGenerator;
    private final List<KeyValue> kvList;
    private final List<SequenceValue> svList;

    private Long lastActualSequence;
    private boolean allSameActualSequence;

    public SequenceFieldReorderFunction(
            SequenceGenerator sequenceGenerator, RowType keyType, RowType valueType) {
        this.sequenceGenerator = sequenceGenerator;
        this.keySerializer = new InternalRowSerializer(keyType);
        this.valueSerializer = new InternalRowSerializer(valueType);
        this.kvList = new LinkedList<>();
        this.svList = new ArrayList<>();
        this.allSameActualSequence = true;
    }

    @Override
    public void reset() {
        kvList.clear();
        svList.clear();
    }

    @Override
    public void add(KeyValue kv) {
        long autoSequence = kv.sequenceNumber();
        KeyValue copy =
                new KeyValue().replace(keySerializer.copy(kv.key()), autoSequence, null, null);
        Long actualSequence = sequenceGenerator.generateNullable(kv.value(), kv.valueKind());
        if (actualSequence != null) {
            // add for sorting.
            svList.add(
                    new SequenceValue(
                            autoSequence,
                            actualSequence,
                            kv.valueKind(),
                            valueSerializer.copy(kv.value()),
                            kv.level()));
            if (lastActualSequence == null) {
                lastActualSequence = actualSequence;
            } else if (actualSequence.compareTo(lastActualSequence) != 0) {
                allSameActualSequence = false;
            }
        } else {
            // when the sequence field is null, it will retain the order in which it flows in.
            copy.replaceValueKind(kv.valueKind())
                    .replaceValue(valueSerializer.copy(kv.value()))
                    .setLevel(kv.level());
        }
        // add all.
        kvList.add(copy);
    }

    /**
     * Return the reordered KeyValues.
     *
     * <pre>
     * example format:
     * valueKind,sequence,key,value(sequence field)
     * add:                     return:
     * INSERT,22,1,58           INSERT,22,1,7
     * DELETE,23,1,58           INSERT,23,1,31
     * INSERT,24,1,31           DELETE,24,1,31
     * DELETE,25,1,31           INSERT,25,1,40
     * INSERT,26,1,76           INSERT,26,1,58
     * INSERT,27,1,7            DELETE,27,1,58
     * INSERT,28,1,null         INSERT,28,1,null
     * INSERT,29,1,40           INSERT,29,1,76
     * </pre>
     */
    @Override
    public List<KeyValue> getResult() {
        if (!allSameActualSequence && svList.size() > 1) {
            svList.sort(
                    Comparator.comparingLong(SequenceValue::getActualSequence)
                            .thenComparingLong(SequenceValue::getAutoSequence));
        }

        int index = 0;
        for (KeyValue kv : kvList) {
            if (kv.value() == null) {
                SequenceValue sv = svList.get(index++);
                kv.replaceValueKind(sv.getValueKind())
                        .replaceValue(sv.getValue())
                        .setLevel(sv.getLevel());
            }
        }
        return kvList;
    }

    public static ReorderFunctionFactory<KeyValue> factory(
            RowType keyType, RowType valueType, CoreOptions options) {
        return new Factory(keyType, valueType, options);
    }

    private static class Factory implements ReorderFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;
        private final RowType keyType;
        private final RowType valueType;
        @Nullable private final SequenceGenerator sequenceGenerator;
        private final CoreOptions options;

        public Factory(RowType keyType, RowType valueType, CoreOptions options) {
            this.keyType = keyType;
            this.valueType = valueType;
            this.options = options;
            this.sequenceGenerator = SequenceGenerator.create(valueType, options);
        }

        @Override
        public ReorderFunction<KeyValue> create(@Nullable int[][] projection) {
            if (sequenceGenerator != null) {
                if (projection != null) {
                    int[] projects = Projection.of(projection).toTopLevelIndexes();
                    for (int i = 0; i < projects.length; i++) {
                        if (projects[i] == sequenceGenerator.index()) {
                            return new SequenceFieldReorderFunction(
                                    new SequenceGenerator(i, sequenceGenerator.fieldType()),
                                    keyType,
                                    Projection.of(projection).project(valueType));
                        }
                    }
                } else {
                    return new SequenceFieldReorderFunction(sequenceGenerator, keyType, valueType);
                }
            }

            return null;
        }

        @Override
        public MergeFunctionFactory.AdjustedProjection adjustProjection(
                @Nullable int[][] projection) {
            if (sequenceGenerator == null) {
                return new MergeFunctionFactory.AdjustedProjection(projection, null);
            }

            if (projection == null) {
                return new MergeFunctionFactory.AdjustedProjection(null, null);
            }
            int[] topProjects = Projection.of(projection).toTopLevelIndexes();
            Set<Integer> indexSet = Arrays.stream(topProjects).boxed().collect(Collectors.toSet());

            int[] allProjects =
                    Stream.concat(
                                    Arrays.stream(topProjects).boxed(),
                                    indexSet.contains(sequenceGenerator.index())
                                            ? Stream.empty()
                                            : Stream.of(sequenceGenerator.index()))
                            .mapToInt(Integer::intValue)
                            .toArray();
            int[][] pushdown = Projection.of(allProjects).toNestedIndexes();
            int[][] outer =
                    Projection.of(IntStream.range(0, topProjects.length).toArray())
                            .toNestedIndexes();
            return new MergeFunctionFactory.AdjustedProjection(pushdown, outer);
        }
    }

    private static class SequenceValue {

        private final Long autoSequence;
        private final Long actualSequence;
        private final RowKind valueKind;
        private final InternalRow value;
        private final int level;

        public SequenceValue(
                Long autoSequence,
                Long actualSequence,
                RowKind valueKind,
                InternalRow value,
                int level) {
            this.autoSequence = autoSequence;
            this.actualSequence = actualSequence;
            this.valueKind = valueKind;
            this.value = value;
            this.level = level;
        }

        public Long getAutoSequence() {
            return autoSequence;
        }

        public Long getActualSequence() {
            return actualSequence;
        }

        public RowKind getValueKind() {
            return valueKind;
        }

        public InternalRow getValue() {
            return value;
        }

        public int getLevel() {
            return level;
        }
    }
}
