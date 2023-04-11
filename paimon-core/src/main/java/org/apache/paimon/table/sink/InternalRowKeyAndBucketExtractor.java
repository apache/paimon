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

package org.apache.paimon.table.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;

/** {@link KeyAndBucketExtractor} for {@link InternalRow}. */
public class InternalRowKeyAndBucketExtractor implements KeyAndBucketExtractor<InternalRow> {

    private final int numBuckets;
    private final boolean sameBucketKeyAndTrimmedPrimaryKey;

    private final Projection partitionProjection;
    private final Projection bucketKeyProjection;
    private final Projection trimmedPrimaryKeyProjection;
    private final Projection logPrimaryKeyProjection;

    private InternalRow record;

    private BinaryRow partition;
    private BinaryRow bucketKey;
    private Integer bucket;
    private BinaryRow trimmedPrimaryKey;
    private BinaryRow logPrimaryKey;

    public InternalRowKeyAndBucketExtractor(TableSchema schema) {
        numBuckets = new CoreOptions(schema.options()).bucket();
        sameBucketKeyAndTrimmedPrimaryKey = schema.bucketKeys().equals(schema.trimmedPrimaryKeys());

        partitionProjection =
                CodeGenUtils.newProjection(
                        schema.logicalRowType(), schema.projection(schema.partitionKeys()));
        bucketKeyProjection =
                CodeGenUtils.newProjection(
                        schema.logicalRowType(), schema.projection(schema.bucketKeys()));
        trimmedPrimaryKeyProjection =
                CodeGenUtils.newProjection(
                        schema.logicalRowType(), schema.projection(schema.trimmedPrimaryKeys()));
        logPrimaryKeyProjection =
                CodeGenUtils.newProjection(
                        schema.logicalRowType(), schema.projection(schema.primaryKeys()));
    }

    @Override
    public void setRecord(InternalRow record) {
        this.record = record;

        this.partition = null;
        this.bucketKey = null;
        this.bucket = null;
        this.trimmedPrimaryKey = null;
        this.logPrimaryKey = null;
    }

    @Override
    public BinaryRow partition() {
        if (partition == null) {
            partition = partitionProjection.apply(record);
        }
        return partition;
    }

    @Override
    public int bucket() {
        if (bucketKey == null) {
            bucketKey = bucketKeyProjection.apply(record);
            if (sameBucketKeyAndTrimmedPrimaryKey) {
                trimmedPrimaryKey = bucketKey;
            }
        }
        if (bucket == null) {
            bucket =
                    KeyAndBucketExtractor.bucket(
                            KeyAndBucketExtractor.bucketKeyHashCode(bucketKey), numBuckets);
        }
        return bucket;
    }

    @Override
    public BinaryRow trimmedPrimaryKey() {
        if (trimmedPrimaryKey == null) {
            trimmedPrimaryKey = trimmedPrimaryKeyProjection.apply(record);
            if (sameBucketKeyAndTrimmedPrimaryKey) {
                bucketKey = trimmedPrimaryKey;
            }
        }
        return trimmedPrimaryKey;
    }

    @Override
    public BinaryRow logPrimaryKey() {
        if (logPrimaryKey == null) {
            assert logPrimaryKeyProjection != null;
            logPrimaryKey = logPrimaryKeyProjection.apply(record);
        }
        return logPrimaryKey;
    }
}
