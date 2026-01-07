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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.partition.PartitionTimeExtractor;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.RowType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Utils for chain table. */
public class ChainTableUtils {

    public static boolean isChainTable(Map<String, String> tblOptions) {
        return CoreOptions.fromMap(tblOptions).isChainTable();
    }

    public static Map<BinaryRow, BinaryRow> findFirstLatestPartitions(
            List<BinaryRow> sortedSourcePartitions,
            List<BinaryRow> sortedTargetPartitions,
            RecordComparator partitionComparator) {
        Map<BinaryRow, BinaryRow> partitionMapping = new HashMap<>();
        int targetIndex = 0;
        for (BinaryRow sourceRow : sortedSourcePartitions) {
            BinaryRow firstSmaller;
            while (targetIndex < sortedTargetPartitions.size()
                    && partitionComparator.compare(
                                    sortedTargetPartitions.get(targetIndex), sourceRow)
                            < 0) {
                targetIndex++;
            }
            firstSmaller = (targetIndex > 0) ? sortedTargetPartitions.get(targetIndex - 1) : null;
            partitionMapping.put(sourceRow, firstSmaller);
        }
        return partitionMapping;
    }

    public static List<BinaryRow> getDeltaPartitions(
            BinaryRow beginPartition,
            BinaryRow endPartition,
            List<String> partitionColumns,
            RowType partType,
            CoreOptions options,
            RecordComparator partitionComparator,
            InternalRowPartitionComputer partitionComputer) {
        InternalRowSerializer serializer = new InternalRowSerializer(partType);
        List<BinaryRow> deltaPartitions = new ArrayList<>();
        boolean isDailyPartition = partitionColumns.size() == 1;
        List<String> startPartitionValues =
                new ArrayList<>(partitionComputer.generatePartValues(beginPartition).values());
        List<String> endPartitionValues =
                new ArrayList<>(partitionComputer.generatePartValues(endPartition).values());
        PartitionTimeExtractor timeExtractor =
                new PartitionTimeExtractor(
                        options.partitionTimestampPattern(), options.partitionTimestampFormatter());
        LocalDateTime stratPartitionTime =
                timeExtractor.extract(partitionColumns, startPartitionValues);
        LocalDateTime candidateTime = stratPartitionTime;
        LocalDateTime endPartitionTime =
                timeExtractor.extract(partitionColumns, endPartitionValues);
        while (!candidateTime.isAfter(endPartitionTime)) {
            if (isDailyPartition) {
                if (candidateTime.isAfter(stratPartitionTime)) {
                    deltaPartitions.add(
                            serializer
                                    .toBinaryRow(
                                            InternalRowPartitionComputer.convertSpecToInternalRow(
                                                    calPartValues(
                                                            candidateTime,
                                                            partitionColumns,
                                                            options.partitionTimestampPattern(),
                                                            options.partitionTimestampFormatter()),
                                                    partType,
                                                    options.partitionDefaultName()))
                                    .copy());
                }
            } else {
                for (int hour = 0; hour <= 23; hour++) {
                    candidateTime = candidateTime.toLocalDate().atStartOfDay().plusHours(hour);
                    BinaryRow candidatePartition =
                            serializer
                                    .toBinaryRow(
                                            InternalRowPartitionComputer.convertSpecToInternalRow(
                                                    calPartValues(
                                                            candidateTime,
                                                            partitionColumns,
                                                            options.partitionTimestampPattern(),
                                                            options.partitionTimestampFormatter()),
                                                    partType,
                                                    options.partitionDefaultName()))
                                    .copy();
                    if (partitionComparator.compare(candidatePartition, beginPartition) > 0
                            && partitionComparator.compare(candidatePartition, endPartition) <= 0) {
                        deltaPartitions.add(candidatePartition);
                    }
                }
            }
            candidateTime = candidateTime.toLocalDate().plusDays(1).atStartOfDay();
        }
        return deltaPartitions;
    }

    public static Predicate createTriangularPredicate(
            BinaryRow binaryRow,
            RowDataToObjectArrayConverter converter,
            BiFunction<Integer, Object, Predicate> innerFunc,
            BiFunction<Integer, Object, Predicate> outerFunc) {
        List<Predicate> fieldPredicates = new ArrayList<>();
        Object[] partitionObjects = converter.convert(binaryRow);
        for (int i = 0; i < converter.getArity(); i++) {
            List<Predicate> andConditions = new ArrayList<>();
            for (int j = 0; j < i; j++) {
                Object o = partitionObjects[j];
                andConditions.add(innerFunc.apply(j, o));
            }
            Object currentValue = partitionObjects[i];
            andConditions.add(outerFunc.apply(i, currentValue));
            fieldPredicates.add(PredicateBuilder.and(andConditions));
        }
        return PredicateBuilder.or(fieldPredicates);
    }

    public static Predicate createLinearPredicate(
            BinaryRow binaryRow,
            RowDataToObjectArrayConverter converter,
            BiFunction<Integer, Object, Predicate> func) {
        List<Predicate> fieldPredicates = new ArrayList<>();
        Object[] partitionObjects = converter.convert(binaryRow);
        for (int i = 0; i < converter.getArity(); i++) {
            fieldPredicates.add(func.apply(i, partitionObjects[i]));
        }
        return PredicateBuilder.and(fieldPredicates);
    }

    public static LinkedHashMap<String, String> calPartValues(
            LocalDateTime dateTime,
            List<String> partitionKeys,
            String timestampPattern,
            String timestampFormatter) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timestampFormatter);
        String formattedDateTime = dateTime.format(formatter);
        Pattern keyPattern = Pattern.compile("\\$(\\w+)");
        Matcher keyMatcher = keyPattern.matcher(timestampPattern);
        List<String> keyOrder = new ArrayList<>();
        StringBuilder regexBuilder = new StringBuilder();
        int lastPosition = 0;
        while (keyMatcher.find()) {
            regexBuilder.append(
                    Pattern.quote(timestampPattern.substring(lastPosition, keyMatcher.start())));
            regexBuilder.append("(.+)");
            keyOrder.add(keyMatcher.group(1));
            lastPosition = keyMatcher.end();
        }
        regexBuilder.append(Pattern.quote(timestampPattern.substring(lastPosition)));

        Matcher valueMatcher = Pattern.compile(regexBuilder.toString()).matcher(formattedDateTime);
        if (!valueMatcher.matches() || valueMatcher.groupCount() != keyOrder.size()) {
            throw new IllegalArgumentException(
                    "Formatted datetime does not match timestamp pattern");
        }

        Map<String, String> keyValues = new HashMap<>();
        for (int i = 0; i < keyOrder.size(); i++) {
            keyValues.put(keyOrder.get(i), valueMatcher.group(i + 1));
        }
        List<String> values =
                partitionKeys.stream()
                        .map(key -> keyValues.getOrDefault(key, ""))
                        .collect(Collectors.toList());
        LinkedHashMap<String, String> res = new LinkedHashMap<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            res.put(partitionKeys.get(i), values.get(i));
        }
        return res;
    }
}
