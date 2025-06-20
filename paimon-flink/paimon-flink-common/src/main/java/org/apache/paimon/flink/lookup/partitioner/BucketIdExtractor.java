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

package org.apache.paimon.flink.lookup.partitioner;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.bucket.BucketFunction;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * {@link BucketIdExtractor} is used to extract the bucket id from join keys. It requires that the
 * join keys contain all bucket keys and projects the bucket key fields from the given join key row.
 */
public class BucketIdExtractor implements Serializable {

    private final int numBuckets;

    private final TableSchema tableSchema;

    private final List<String> joinKeyFieldNames;

    private final List<String> bucketKeyFieldNames;

    private BucketFunction bucketFunction;

    private Projection bucketKeyProjection;

    public BucketIdExtractor(
            int numBuckets,
            TableSchema tableSchema,
            List<String> joinKeyFieldNames,
            List<String> bucketKeyFieldNames) {
        checkState(
                new HashSet<>(joinKeyFieldNames).containsAll(bucketKeyFieldNames),
                "The join keys must contain all bucket keys.");
        checkState(numBuckets > 0, "Number of buckets should be positive.");
        this.numBuckets = numBuckets;
        this.joinKeyFieldNames = joinKeyFieldNames;
        this.bucketKeyFieldNames = bucketKeyFieldNames;
        this.tableSchema = tableSchema;
    }

    public int extractBucketId(RowData joinKeyRow) {
        checkState(joinKeyRow.getArity() == joinKeyFieldNames.size());
        if (bucketKeyProjection == null) {
            bucketKeyProjection = generateBucketKeyProjection();
        }
        if (bucketFunction == null) {
            bucketFunction =
                    BucketFunction.create(
                            new CoreOptions(tableSchema.options()),
                            tableSchema.logicalBucketKeyType());
        }
        FlinkRowWrapper internalRow = new FlinkRowWrapper(joinKeyRow);
        BinaryRow bucketKey = bucketKeyProjection.apply(internalRow);
        int bucket = bucketFunction.bucket(bucketKey, numBuckets);
        checkState(bucket < numBuckets);
        return bucket;
    }

    private Projection generateBucketKeyProjection() {
        int[] bucketKeyIndexes =
                bucketKeyFieldNames.stream().mapToInt(joinKeyFieldNames::indexOf).toArray();
        List<DataField> joinKeyDataFields =
                joinKeyFieldNames.stream()
                        .map(
                                joinKeyFieldName ->
                                        tableSchema
                                                .fields()
                                                .get(
                                                        tableSchema
                                                                .fieldNames()
                                                                .indexOf(joinKeyFieldName)))
                        .collect(Collectors.toList());
        return CodeGenUtils.newProjection(new RowType(joinKeyDataFields), bucketKeyIndexes);
    }
}
