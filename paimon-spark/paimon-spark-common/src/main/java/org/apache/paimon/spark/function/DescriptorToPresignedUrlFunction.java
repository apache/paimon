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

import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;

/** Spark scalar function placeholder replaced with a table-scoped function during analysis. */
public class DescriptorToPresignedUrlFunction implements ScalarFunction<UTF8String>, Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean ignoreErrors;
    private final String catalogName;
    private final DayTimeIntervalType intervalType;

    public DescriptorToPresignedUrlFunction(boolean ignoreErrors) {
        this(ignoreErrors, null, DataTypes.createDayTimeIntervalType());
    }

    public DescriptorToPresignedUrlFunction(boolean ignoreErrors, String catalogName) {
        this(ignoreErrors, catalogName, DataTypes.createDayTimeIntervalType());
    }

    public DescriptorToPresignedUrlFunction(
            boolean ignoreErrors, String catalogName, DayTimeIntervalType intervalType) {
        this.ignoreErrors = ignoreErrors;
        this.catalogName = catalogName;
        this.intervalType = intervalType;
    }

    @Override
    public DataType[] inputTypes() {
        return new DataType[] {
            DataTypes.StringType, DataTypes.BinaryType, DataTypes.StringType, intervalType
        };
    }

    @Override
    public DataType resultType() {
        return DataTypes.StringType;
    }

    public UTF8String invoke(
            UTF8String sourceTable, byte[] descriptor, UTF8String extension, long validityMicros) {
        if (sourceTable == null || descriptor == null || extension == null) {
            return null;
        }
        throw new UnsupportedOperationException(
                "Function '" + name() + "' requires a non-null literal sourceTable.");
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    public boolean ignoreErrors() {
        return ignoreErrors;
    }

    public String catalogName() {
        return catalogName;
    }

    @Override
    public String name() {
        return DescriptorToPresignedUrlUnbound.functionName(ignoreErrors);
    }

    @Override
    public String canonicalName() {
        return "paimon." + name() + "(string, binary, string, interval day to second)";
    }
}
