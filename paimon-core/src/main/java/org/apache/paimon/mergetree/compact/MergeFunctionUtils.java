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
import org.apache.paimon.mergetree.SequenceGenerator;
import org.apache.paimon.mergetree.SequenceGenerator.Seq;
import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Projection;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.FIELDS_PREFIX;
import static org.apache.paimon.mergetree.compact.PartialUpdateMergeFunction.SEQUENCE_GROUP;

/** Utils for merge function. */
public class MergeFunctionUtils {

    public static Map<Integer, SequenceGenerator> getSequenceGenerator(
            CoreOptions options, List<String> fieldNames, RowType rowType) {
        Map<Integer, SequenceGenerator> fieldSequences = new HashMap<>();
        for (Map.Entry<String, String> entry : options.toMap().entrySet()) {
            String k = entry.getKey();
            String v = entry.getValue();
            if (k.startsWith(FIELDS_PREFIX) && k.endsWith(SEQUENCE_GROUP)) {
                String sequenceFieldName =
                        k.substring(
                                FIELDS_PREFIX.length() + 1,
                                k.length() - SEQUENCE_GROUP.length() - 1);
                SequenceGenerator sequenceGen = new SequenceGenerator(sequenceFieldName, rowType);
                Arrays.stream(v.split(","))
                        .map(
                                fieldName -> {
                                    int field = fieldNames.indexOf(fieldName);
                                    if (field == -1) {
                                        throw new IllegalArgumentException(
                                                String.format(
                                                        "Field %s can not be found in table schema",
                                                        fieldName));
                                    }
                                    return field;
                                })
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
        return fieldSequences;
    }

    public static Map<Integer, SequenceGenerator> projectSequence(
            int[][] projection, Map<Integer, SequenceGenerator> fieldSequences) {
        if (projection == null) {
            return fieldSequences;
        }
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
                                    new SequenceGenerator(newSequenceId, sequence.fieldType()));
                        }
                    }
                });
        return projectedSequences;
    }

    public static Seq[] toSeq(
            Map<Integer, SequenceGenerator> fieldSequences, FieldAggregator[] fieldAggregators) {
        Seq[] sequences = new Seq[fieldAggregators.length];
        for (Map.Entry<Integer, SequenceGenerator> entry : fieldSequences.entrySet()) {
            int index = entry.getKey();
            Seq seq;
            if (fieldAggregators[index] != null && fieldAggregators[index].requireSequence()) {
                // sequence for the exclusive aggregation function
                sequences[index] = new Seq(entry.getValue(), true);
                // The sequence field itself.
                sequences[entry.getValue().index()] = new Seq(entry.getValue(), true);
            } else {
                seq = new Seq(entry.getValue(), false);
                if (sequences[index] == null) {
                    sequences[index] = seq;
                }
            }
        }
        return sequences;
    }

    public static Map<Integer, Set<Integer>> seqIndex2Field(
            Map<Integer, SequenceGenerator> generators) {
        Map<Integer, Set<Integer>> seq2Field = new HashMap<>();
        for (Map.Entry<Integer, SequenceGenerator> entry : generators.entrySet()) {
            int seqIndex = entry.getValue().index();
            seq2Field.computeIfAbsent(seqIndex, k -> new HashSet<>());
            Set<Integer> fields = seq2Field.get(seqIndex);
            fields.add(entry.getKey());
        }
        return seq2Field;
    }

    /** The required sequence's aggregator can not share sequence filed. */
    public static void validateSequence(
            FieldAggregator[] aggregators,
            Map<Integer, SequenceGenerator> sequences,
            List<String> fieldNames) {
        Map<Integer, Set<Integer>> seq2Field = seqIndex2Field(sequences);
        for (int i = 0; i < aggregators.length; i++) {
            FieldAggregator aggregator = aggregators[i];
            if (aggregator != null && aggregator.requireSequence()) {
                SequenceGenerator seqGen = sequences.get(i);
                if (seqGen == null) {
                    continue;
                    // here we should allow, but for compatibility
                }
                int seqIndex = seqGen.index();
                Set<Integer> values = seq2Field.get(seqIndex);
                // remove self.
                values.remove(seqIndex);
                if (values.size() >= 2) {
                    throw new RuntimeException(
                            String.format(
                                    "Should not share sequence field in [%s] due to %s",
                                    values.stream()
                                            .map(fieldNames::get)
                                            .collect(Collectors.joining(",")),
                                    aggregator.name()));
                }
            }
        }
    }
}
