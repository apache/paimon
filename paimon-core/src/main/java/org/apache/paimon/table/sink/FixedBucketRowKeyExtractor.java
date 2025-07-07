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
import org.apache.paimon.bucket.BucketFunction;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;

/** {@link KeyAndBucketExtractor} for {@link InternalRow}. */
public class FixedBucketRowKeyExtractor extends RowKeyExtractor {

    private final int numBuckets;
    private final boolean sameBucketKeyAndTrimmedPrimaryKey;
    private final Projection bucketKeyProjection;

    private BinaryRow reuseBucketKey;
    private Integer reuseBucket;
    private final BucketFunction bucketFunction;

    public FixedBucketRowKeyExtractor(TableSchema schema) {
        super(schema);
        numBuckets = new CoreOptions(schema.options()).bucket();
        bucketFunction =
                BucketFunction.create(
                        new CoreOptions(schema.options()), schema.logicalBucketKeyType());
        sameBucketKeyAndTrimmedPrimaryKey = schema.bucketKeys().equals(schema.trimmedPrimaryKeys());
        bucketKeyProjection =
                CodeGenUtils.newProjection(
                        schema.logicalRowType(), schema.projection(schema.bucketKeys()));
    }

    @Override
    public void setRecord(InternalRow record) {
        super.setRecord(record);
        this.reuseBucketKey = null;
        this.reuseBucket = null;
    }

    private BinaryRow bucketKey() {
        if (sameBucketKeyAndTrimmedPrimaryKey) {
            return trimmedPrimaryKey();
        }

        if (reuseBucketKey == null) {
            reuseBucketKey = bucketKeyProjection.apply(record);
        }
        return reuseBucketKey;
    }

    @Override
    public int bucket() {
        BinaryRow bucketKey = bucketKey();
        if (reuseBucket == null) {
            reuseBucket = bucketFunction.bucket(bucketKey, numBuckets);
        }
        return reuseBucket;
    }
}
