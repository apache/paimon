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

package org.apache.paimon.spark.utils;

import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionUtils;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ParameterUtils;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utils for Spark procedures. */
public class SparkProcedureUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SparkProcedureUtils.class);

    @Nullable
    public static PartitionPredicate convertToPartitionPredicate(
            @Nullable String where,
            RowType partitionType,
            SparkSession sparkSession,
            DataSourceV2Relation relation) {
        if (StringUtils.isNullOrWhitespaceOnly(where)) {
            return null;
        }

        List<String> partitionKeys = partitionType.getFieldNames();
        checkArgument(
                !partitionKeys.isEmpty(),
                "Table should be a partitioned table when using partition predicate.");

        Expression condition = ExpressionUtils.resolveFilter(sparkSession, relation, where);
        checkArgument(
                ExpressionUtils.isValidPredicate(
                        sparkSession, condition, partitionKeys.toArray(new String[0])),
                "Only partition predicate is supported, your predicate is %s, but partition keys are %s",
                condition,
                partitionKeys);

        Predicate predicate =
                ExpressionUtils.convertConditionToPaimonPredicate(
                                condition, ((LogicalPlan) relation).output(), partitionType, false)
                        .get();
        return PartitionPredicate.fromPredicate(partitionType, predicate);
    }

    public static int readParallelism(List<?> groupedTasks, SparkSession spark) {
        int sparkParallelism =
                Math.max(
                        spark.sparkContext().defaultParallelism(),
                        spark.sessionState().conf().numShufflePartitions());
        int readParallelism = Math.min(groupedTasks.size(), sparkParallelism);
        if (sparkParallelism > readParallelism) {
            LOG.warn(
                    "Spark default parallelism ({}) is greater than bucket or task parallelism ({}),"
                            + "we use {} as the final read parallelism",
                    sparkParallelism,
                    readParallelism,
                    readParallelism);
        }
        return readParallelism;
    }

    public static String toWhere(String partitions) {
        List<Map<String, String>> maps = ParameterUtils.getPartitions(partitions.split(";"));

        return maps.stream()
                .map(
                        a ->
                                a.entrySet().stream()
                                        .map(entry -> entry.getKey() + "=" + entry.getValue())
                                        .reduce((s0, s1) -> s0 + " AND " + s1))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(a -> "(" + a + ")")
                .reduce((a, b) -> a + " OR " + b)
                .orElse(null);
    }
}
