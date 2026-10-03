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

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.LeafBinaryFunction;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataType;
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

    private static final String PARTITIONS_TABLE_PARTITION_FIELD = "partition";

    /**
     * Converts equality and IN filters on the {@code partition} column of PartitionsTable, such as
     * {@code dt=20260410/region=1}, into a partition filter for manifest pruning.
     *
     * <p>Returns {@code null} unless every literal parses and renders back to itself, so pruning
     * never drops a partition that the string predicate itself would match.
     */
    @Nullable
    public static Predicate partitionsTableFilter(
            @Nullable Predicate predicate,
            List<String> partitionKeys,
            RowType partitionType,
            String defaultPartitionName) {
        List<Object> literals = partitionsTableLiterals(predicate);
        if (literals == null || literals.isEmpty()) {
            return null;
        }
        PredicateBuilder builder = new PredicateBuilder(partitionType);
        List<Predicate> orPredicates = new ArrayList<>();
        for (Object literal : literals) {
            Predicate partitionFilter =
                    partitionsTableSpecFilter(
                            literal, partitionKeys, partitionType, defaultPartitionName, builder);
            if (partitionFilter == null) {
                return null;
            }
            orPredicates.add(partitionFilter);
        }
        return PredicateBuilder.or(orPredicates);
    }

    /** Literals of the first conjunct that is an equality, IN or OR of equalities on partition. */
    @Nullable
    private static List<Object> partitionsTableLiterals(@Nullable Predicate predicate) {
        for (Predicate conjunct : PredicateBuilder.splitAnd(predicate)) {
            List<Object> literals = new ArrayList<>();
            for (Predicate disjunct : PredicateBuilder.splitOr(conjunct)) {
                if (!isPartitionsTableEqualOrIn(disjunct)) {
                    literals = null;
                    break;
                }
                literals.addAll(((LeafPredicate) disjunct).literals());
            }
            if (literals != null) {
                return literals;
            }
        }
        return null;
    }

    private static boolean isPartitionsTableEqualOrIn(Predicate predicate) {
        if (!(predicate instanceof LeafPredicate)) {
            return false;
        }
        LeafPredicate leaf = (LeafPredicate) predicate;
        return (leaf.function() instanceof Equal || leaf.function() instanceof In)
                && leaf.fieldRefOptional()
                        .map(ref -> PARTITIONS_TABLE_PARTITION_FIELD.equals(ref.name()))
                        .orElse(false);
    }

    @Nullable
    private static Predicate partitionsTableSpecFilter(
            @Nullable Object literal,
            List<String> partitionKeys,
            RowType partitionType,
            String defaultPartitionName,
            PredicateBuilder builder) {
        if (literal == null) {
            return null;
        }
        String[] fields = literal.toString().split("/", -1);
        if (fields.length != partitionKeys.size()) {
            return null;
        }
        List<Predicate> predicates = new ArrayList<>();
        for (int i = 0; i < fields.length; i++) {
            String prefix = partitionKeys.get(i) + "=";
            if (!fields[i].startsWith(prefix)) {
                return null;
            }
            String value = fields[i].substring(prefix.length());
            if (defaultPartitionName.equals(value)) {
                predicates.add(builder.isNull(i));
                continue;
            }
            Object parsed = renderableValue(value, partitionType.getTypeAt(i));
            if (parsed == null) {
                return null;
            }
            predicates.add(builder.equal(i, parsed));
        }
        return PredicateBuilder.and(predicates);
    }

    /** Parses a partition value only if PartitionsTable renders it back to the same string. */
    @Nullable
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object renderableValue(String value, DataType type) {
        try {
            Object parsed = TypeUtils.castFromString(value, type);
            if (parsed == null) {
                return null;
            }
            CastExecutor toString = CastExecutors.resolveToString(type);
            return value.equals(toString.cast(parsed).toString()) ? parsed : null;
        } catch (RuntimeException e) {
            return null;
        }
    }

    public static boolean applyPartitionFilter(
            SnapshotReader snapshotReader,
            @Nullable LeafPredicate partitionPredicate,
            List<String> partitionKeys,
            RowType partitionType) {
        if (partitionPredicate == null) {
            return true;
        }

        if (partitionPredicate.function() instanceof Equal) {
            LinkedHashMap<String, String> partSpec =
                    parsePartitionSpec(
                            partitionPredicate.literals().get(0).toString(), partitionKeys);
            if (partSpec == null) {
                return false;
            }
            snapshotReader.withPartitionFilter(partSpec);
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
            if (!orPredicates.isEmpty()) {
                snapshotReader.withPartitionFilter(PredicateBuilder.or(orPredicates));
            }
        } else if (partitionPredicate.function() instanceof LeafBinaryFunction) {
            LinkedHashMap<String, String> partSpec =
                    parsePartitionSpec(
                            partitionPredicate.literals().get(0).toString(), partitionKeys);
            if (partSpec != null) {
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
                snapshotReader.withPartitionFilter(PredicateBuilder.and(predicates));
            }
        }

        return true;
    }

    @Nullable
    public static LinkedHashMap<String, String> parsePartitionSpec(
            String partitionStr, List<String> partitionKeys) {
        if (partitionStr.startsWith("{")) {
            partitionStr = partitionStr.substring(1);
        }
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
}
