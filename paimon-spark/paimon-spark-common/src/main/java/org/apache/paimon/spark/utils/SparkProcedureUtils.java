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
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ParameterUtils;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    @Nullable
    public static PartitionPredicate convertPartitionsToPartitionPredicate(
            @Nullable String partitions, FileStoreTable table, SparkSession spark) {
        // `partitions` is a structured partition spec path such as
        // `dt=2024-01-01,hh=0;dt=2024-01-02,hh=1`, not a SQL expression.
        // Values that are valid Spark SQL literals are normalized with Spark's parser, so quoted
        // partition values follow Spark partition spec behavior. Values that are not literals are
        // kept as path-style typed strings for backward compatibility.
        //
        // Invalid partition keys are rejected explicitly here so procedures can fail with a clear
        // error message before converting values to Paimon internal literals.
        if (StringUtils.isNullOrWhitespaceOnly(partitions)) {
            return null;
        }

        RowType partitionType = table.schema().logicalPartitionType();
        List<String> partitionKeys = partitionType.getFieldNames();
        checkArgument(
                !partitionKeys.isEmpty(),
                "Table should be a partitioned table when using partition predicate.");

        List<Map<String, String>> partitionSpecs =
                ParameterUtils.getPartitions(partitions.split(";"));
        validatePartitionKeys(partitionSpecs, partitionKeys);
        partitionSpecs = normalizePartitionValuesWithSparkParser(spark, partitionSpecs);

        Predicate predicate =
                PredicateBuilder.partitions(
                        partitionSpecs, partitionType, table.coreOptions().partitionDefaultName());
        return PartitionPredicate.fromPredicate(partitionType, predicate);
    }

    private static void validatePartitionKeys(
            List<Map<String, String>> partitionSpecs, List<String> partitionKeys) {
        Set<String> invalidKeys = new HashSet<>();
        for (Map<String, String> partitionSpec : partitionSpecs) {
            for (String partitionKey : partitionSpec.keySet()) {
                if (!partitionKeys.contains(partitionKey)) {
                    invalidKeys.add(partitionKey);
                }
            }
        }

        checkArgument(
                invalidKeys.isEmpty(),
                "Partition keys %s are invalid. Available partition keys are %s",
                invalidKeys,
                partitionKeys);
    }

    private static List<Map<String, String>> normalizePartitionValuesWithSparkParser(
            SparkSession spark, List<Map<String, String>> partitionSpecs) {
        return partitionSpecs.stream()
                .map(partitionSpec -> parsePartitionSpec(spark, partitionSpec, false))
                .collect(java.util.stream.Collectors.toList());
    }

    /**
     * Parse static partition spec values by evaluating them as Spark SQL literal expressions. This
     * keeps overwrite partition handling aligned with Spark SQL partition specs, while rejecting
     * non-literal or null values.
     *
     * @param spark the Spark session
     * @param partitionSpec the partition spec with raw values (e.g., {"date": "\"20260225\""})
     * @return the static partition map with parsed literal values (e.g., {"date": "20260225"})
     */
    public static Map<String, String> parseStaticPartition(
            SparkSession spark, Map<String, String> partitionSpec) {
        return parsePartitionSpec(spark, partitionSpec, true);
    }

    private static Map<String, String> parsePartitionSpec(
            SparkSession spark, Map<String, String> partitionSpec, boolean requireLiteral) {
        Map<String, String> staticPartition = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : partitionSpec.entrySet()) {
            staticPartition.put(
                    entry.getKey(), parsePartitionValue(spark, entry.getValue(), requireLiteral));
        }
        return staticPartition;
    }

    private static String parsePartitionValue(
            SparkSession spark, String value, boolean requireLiteral) {
        Expression expr;
        try {
            expr = spark.sessionState().sqlParser().parseExpression(value);
        } catch (Exception e) {
            if (requireLiteral) {
                throw new RuntimeException(e);
            }
            return value;
        }
        if (!(expr instanceof Literal)) {
            checkArgument(
                    !requireLiteral,
                    "Partition value must be a literal expression, but got: %s",
                    value);
            return value;
        }

        Object literalValue = ((Literal) expr).value();
        if (literalValue == null) {
            checkArgument(!requireLiteral, "Partition value cannot be null");
            return value;
        }
        return literalValue.toString();
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
