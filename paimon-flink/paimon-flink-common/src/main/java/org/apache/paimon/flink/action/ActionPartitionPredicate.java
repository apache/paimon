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

package org.apache.paimon.flink.action;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.predicate.SimpleSqlPredicateConvertor;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.PartitionPredicateVisitor;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateProjectionConverter;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.partition.PartitionPredicate.createBinaryPartitions;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Creates partition predicates shared by table actions. */
final class ActionPartitionPredicate {

    private ActionPartitionPredicate() {}

    static PartitionPredicate create(
            FileStoreTable table,
            List<Map<String, String>> partitions,
            String whereSql,
            String actionName)
            throws Exception {
        checkArgument(
                partitions == null || whereSql == null,
                "partitions and where cannot be used together.");
        Predicate predicate = null;
        RowType partitionType = table.rowType().project(table.partitionKeys());
        String partitionDefaultName = table.coreOptions().partitionDefaultName();
        if (partitions != null) {
            boolean fullMode =
                    partitions.stream()
                            .allMatch(part -> part.size() == partitionType.getFieldCount());
            if (fullMode) {
                List<BinaryRow> binaryPartitions =
                        createBinaryPartitions(partitions, partitionType, partitionDefaultName);
                return PartitionPredicate.fromMultiple(partitionType, binaryPartitions);
            }
            predicate =
                    partitions.stream()
                            .map(
                                    partition ->
                                            createPartitionPredicate(
                                                    partition,
                                                    table.rowType(),
                                                    partitionDefaultName))
                            .reduce(PredicateBuilder::or)
                            .orElseThrow(
                                    () -> new RuntimeException("Failed to get partition filter."));
        } else if (whereSql != null) {
            predicate =
                    new SimpleSqlPredicateConvertor(table.rowType())
                            .convertSqlToPredicate(whereSql);
        }

        if (predicate != null) {
            PartitionPredicateVisitor partitionPredicateVisitor =
                    new PartitionPredicateVisitor(table.partitionKeys());
            checkArgument(
                    predicate.visit(partitionPredicateVisitor),
                    "Only partition key can be specialized in %s action.",
                    actionName);
            predicate =
                    predicate
                            .visit(
                                    PredicateProjectionConverter.fromProjection(
                                            table.rowType().projectIndexes(table.partitionKeys())))
                            .orElseThrow(
                                    () ->
                                            new RuntimeException(
                                                    "Failed to convert partition predicate."));
        }
        return PartitionPredicate.fromPredicate(partitionType, predicate);
    }
}
