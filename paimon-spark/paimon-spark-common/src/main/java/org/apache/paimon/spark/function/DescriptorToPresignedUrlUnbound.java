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

package org.apache.paimon.spark.function;

import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Function unbound to {@link DescriptorToPresignedUrlFunction}. */
public class DescriptorToPresignedUrlUnbound implements UnboundFunction {

    public static final String NAME = "descriptor_to_presigned_url";
    public static final String TRY_NAME = "try_descriptor_to_presigned_url";

    private final boolean ignoreErrors;
    private final String catalogName;

    public DescriptorToPresignedUrlUnbound(boolean ignoreErrors) {
        this(ignoreErrors, null);
    }

    public DescriptorToPresignedUrlUnbound(boolean ignoreErrors, String catalogName) {
        this.ignoreErrors = ignoreErrors;
        this.catalogName = catalogName;
    }

    @Override
    public BoundFunction bind(StructType inputType) {
        StructField[] fields = inputType.fields();
        if (fields.length != 4) {
            throw new UnsupportedOperationException(
                    "Function '"
                            + name()
                            + "' requires 4 arguments "
                            + "(sourceTable STRING, descriptor BINARY, extension STRING, "
                            + "validity INTERVAL DAY TO SECOND), but found "
                            + fields.length);
        }
        requireType(fields, 0, StringType.class, "first", "STRING");
        requireType(fields, 1, BinaryType.class, "second", "BINARY");
        requireType(fields, 2, StringType.class, "third", "STRING");
        requireType(fields, 3, DayTimeIntervalType.class, "fourth", "INTERVAL DAY TO SECOND");
        return new DescriptorToPresignedUrlFunction(
                ignoreErrors, catalogName, (DayTimeIntervalType) fields[3].dataType());
    }

    private void requireType(
            StructField[] fields,
            int position,
            Class<?> expectedClass,
            String positionName,
            String expectedName) {
        if (!expectedClass.isInstance(fields[position].dataType())) {
            throw new UnsupportedOperationException(
                    "The "
                            + positionName
                            + " argument of '"
                            + name()
                            + "' must be "
                            + expectedName
                            + " type, but found "
                            + fields[position].dataType().simpleString());
        }
    }

    @Override
    public String description() {
        return "Create a presigned URL for a blob descriptor";
    }

    @Override
    public String name() {
        return functionName(ignoreErrors);
    }

    static String functionName(boolean ignoreErrors) {
        return ignoreErrors ? TRY_NAME : NAME;
    }
}
