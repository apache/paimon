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

package org.apache.paimon.spark;

import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.BucketSpec;
import org.apache.paimon.table.FileStoreTable;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.apache.spark.sql.connector.distributions.ClusteredDistribution;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.SortOrder;

import java.util.List;

/** Distribution and ordering requirements of spark write. */
public class SparkWriteRequirement {
    public static final SparkWriteRequirement EMPTY =
            new SparkWriteRequirement(Distributions.unspecified());

    private static final SortOrder[] EMPTY_ORDERING = new SortOrder[0];
    private final Distribution distribution;

    public static SparkWriteRequirement of(FileStoreTable table) {
        BucketSpec bucketSpec = table.bucketSpec();
        BucketMode bucketMode = bucketSpec.getBucketMode();
        switch (bucketMode) {
            case HASH_FIXED:
            case BUCKET_UNAWARE:
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported bucket mode %s", bucketMode));
        }

        List<Expression> clusteringExpressions = Lists.newArrayList();

        List<String> partitionKeys = table.schema().partitionKeys();
        for (String partitionKey : partitionKeys) {
            clusteringExpressions.add(Expressions.identity(quote(partitionKey)));
        }

        if (bucketMode == BucketMode.HASH_FIXED) {
            String[] quotedBucketKeys =
                    bucketSpec.getBucketKeys().stream()
                            .map(SparkWriteRequirement::quote)
                            .toArray(String[]::new);
            clusteringExpressions.add(
                    Expressions.bucket(bucketSpec.getNumBuckets(), quotedBucketKeys));
        }

        if (clusteringExpressions.isEmpty()) {
            return EMPTY;
        }

        ClusteredDistribution distribution =
                Distributions.clustered(
                        clusteringExpressions.toArray(
                                clusteringExpressions.toArray(new Expression[0])));

        return new SparkWriteRequirement(distribution);
    }

    private static String quote(String columnName) {
        return String.format("`%s`", columnName.replace("`", "``"));
    }

    private SparkWriteRequirement(Distribution distribution) {
        this.distribution = distribution;
    }

    public Distribution distribution() {
        return distribution;
    }

    public SortOrder[] ordering() {
        return EMPTY_ORDERING;
    }
}
