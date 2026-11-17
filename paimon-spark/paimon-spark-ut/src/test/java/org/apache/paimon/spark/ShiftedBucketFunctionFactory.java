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

import org.apache.paimon.bucket.BucketFunction;
import org.apache.paimon.bucket.BucketFunctionFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

import java.util.Collections;

/** A schema-sensitive, non-default bucket function for Spark integration tests. */
public class ShiftedBucketFunctionFactory implements BucketFunctionFactory {

    @Override
    public String identifier() {
        return "spark-test-shifted";
    }

    @Override
    public BucketFunction create(RowType bucketKeyType, Options tableOptions) {
        if (!bucketKeyType.getFieldNames().equals(Collections.singletonList("id"))) {
            throw new IllegalArgumentException("Expected the schema's id bucket key");
        }
        return (row, numBuckets) -> Math.floorMod(row.getInt(0) + 1, numBuckets);
    }
}
