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

package org.apache.paimon.utils;

import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.LeafBinaryFunction;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Helper for applying partition predicate pushdown in system tables (BucketsTable, FilesTable,
 * FileKeyRangesTable, PartitionsTable).
 */
public class PartitionPredicateHelper {

    /**
     * Build a partition-typed predicate from a string-based leaf predicate on the "partition"
     * column.
     *
     * @return the predicate on partition fields, or {@code null} if the partition spec is invalid
     *     (indicating no results should be returned)
     */
    @Nullable
    public static Predicate buildPartitionPredicate(
            LeafPredicate partitionPredicate, List<String> partitionKeys, RowType partitionType) {
        if (partitionPredicate.function() instanceof Equal) {
            LinkedHashMap<String, String> partSpec =
                    parsePartitionSpec(
                            partitionPredicate.literals().get(0).toString(), partitionKeys);
            if (partSpec == null) {
                return null;
            }
            PredicateBuilder partBuilder = new PredicateBuilder(partitionType);
            List<Predicate> predicates = new ArrayList<>();
            for (int i = 0; i < partitionKeys.size(); i++) {
                Object value =
                        TypeUtils.castFromString(
                                partSpec.get(partitionKeys.get(i)), partitionType.getTypeAt(i));
                predicates.add(partBuilder.equal(i, value));
            }
            return PredicateBuilder.and(predicates);
        } else if (partitionPredicate.function() instanceof In) {
            List<Predicate> orPredicates = new ArrayList<>();
            PredicateBuilder partBuilder = new PredicateBuilder(partitionType);
            for (Object literal : partitionPredicate.literals()) {
                LinkedHashMap<String, String> partSpec =
                        parsePartitionSpec(literal.toString(), partitionKeys);
                if (partSpec == null) {
                    continue;
                }
                List<Predicate> andPredicates = new ArrayList<>();
                for (int i = 0; i < partitionKeys.size(); i++) {
                    Object value =
                            TypeUtils.castFromString(
                                    partSpec.get(partitionKeys.get(i)), partitionType.getTypeAt(i));
                    andPredicates.add(partBuilder.equal(i, value));
                }
                orPredicates.add(PredicateBuilder.and(andPredicates));
            }
            return orPredicates.isEmpty() ? null : PredicateBuilder.or(orPredicates);
        } else if (partitionPredicate.function() instanceof LeafBinaryFunction) {
            LinkedHashMap<String, String> partSpec =
                    parsePartitionSpec(
                            partitionPredicate.literals().get(0).toString(), partitionKeys);
            if (partSpec == null) {
                return null;
            }
            PredicateBuilder partBuilder = new PredicateBuilder(partitionType);
            List<Predicate> predicates = new ArrayList<>();
            for (int i = 0; i < partitionKeys.size(); i++) {
                Object value =
                        TypeUtils.castFromString(
                                partSpec.get(partitionKeys.get(i)), partitionType.getTypeAt(i));
                predicates.add(
                        new LeafPredicate(
                                partitionPredicate.function(),
                                partitionType.getTypeAt(i),
                                i,
                                partitionKeys.get(i),
                                Collections.singletonList(value)));
            }
            return PredicateBuilder.and(predicates);
        }
        return null;
    }

    public static boolean applyPartitionFilter(
            SnapshotReader snapshotReader,
            @Nullable LeafPredicate partitionPredicate,
            List<String> partitionKeys,
            RowType partitionType) {
        if (partitionPredicate == null) {
            return true;
        }

        Predicate predicate =
                buildPartitionPredicate(partitionPredicate, partitionKeys, partitionType);
        if (predicate == null) {
            return false;
        }
        snapshotReader.withPartitionFilter(predicate);
        return true;
    }

    @Nullable
    public static LinkedHashMap<String, String> parsePartitionSpec(
            String partitionStr, List<String> partitionKeys) {
        // Handle {value1, value2} format (BucketsTable, FilesTable, FileKeyRangesTable)
        if (partitionStr.startsWith("{")) {
            partitionStr = partitionStr.substring(1);
            if (partitionStr.endsWith("}")) {
                partitionStr = partitionStr.substring(0, partitionStr.length() - 1);
            }
            String[] partFields = partitionStr.split(", ");
            if (partitionKeys.size() != partFields.length) {
                return null;
            }
            LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
            for (int i = 0; i < partitionKeys.size(); i++) {
                partSpec.put(partitionKeys.get(i), partFields[i]);
            }
            return partSpec;
        }

        // Handle key=value/key=value format (PartitionsTable)
        String[] partFields = partitionStr.split("/");
        if (partitionKeys.size() != partFields.length) {
            return null;
        }
        LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
        for (String field : partFields) {
            int eqIndex = field.indexOf('=');
            if (eqIndex < 0) {
                return null;
            }
            partSpec.put(field.substring(0, eqIndex), field.substring(eqIndex + 1));
        }
        for (String key : partitionKeys) {
            if (!partSpec.containsKey(key)) {
                return null;
            }
        }
        return partSpec;
    }
}
