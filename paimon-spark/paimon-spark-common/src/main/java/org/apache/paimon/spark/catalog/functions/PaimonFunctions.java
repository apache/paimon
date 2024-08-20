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

package org.apache.paimon.spark.catalog.functions;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

/** Paimon functions. */
public class PaimonFunctions {

    private static final Map<String, UnboundFunction> FUNCTIONS =
            ImmutableMap.of("bucket", new PaimonFunctions.BucketFunction());

    private static final List<String> FUNCTION_NAMES = ImmutableList.copyOf(FUNCTIONS.keySet());

    public static List<String> names() {
        return FUNCTION_NAMES;
    }

    public static UnboundFunction load(String name) {
        return FUNCTIONS.get(name);
    }

    /**
     * For now, we only support report bucket partitioning for table scan. So the case `SELECT
     * bucket(10, col)` would fail since we do not implement {@link
     * org.apache.spark.sql.connector.catalog.functions.ScalarFunction}
     */
    public static class BucketFunction implements UnboundFunction {
        @Override
        public BoundFunction bind(StructType inputType) {
            if (inputType.size() != 2) {
                throw new UnsupportedOperationException(
                        "Wrong number of inputs (expected numBuckets and value)");
            }

            StructField numBucket = inputType.fields()[0];
            StructField bucketField = inputType.fields()[1];
            checkArgument(
                    numBucket.dataType() == IntegerType,
                    "bucket number field must be integer type");

            return new BoundFunction() {
                @Override
                public DataType[] inputTypes() {
                    return new DataType[] {IntegerType, bucketField.dataType()};
                }

                @Override
                public DataType resultType() {
                    return IntegerType;
                }

                @Override
                public String name() {
                    return "bucket";
                }

                @Override
                public String canonicalName() {
                    // We have to override this method to make it support canonical equivalent
                    return "paimon.bucket(" + bucketField.dataType().catalogString() + ", int)";
                }
            };
        }

        @Override
        public String description() {
            return name();
        }

        @Override
        public String name() {
            return "bucket";
        }
    }
}
