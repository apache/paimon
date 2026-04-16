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

import org.apache.paimon.data.BlobReference;

import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;

/** Spark scalar function that constructs a serialized {@link BlobReference}. */
public class BlobReferenceSparkFunction implements ScalarFunction<byte[]>, Serializable {

    @Override
    public DataType[] inputTypes() {
        return new DataType[] {DataTypes.StringType, DataTypes.IntegerType, DataTypes.LongType};
    }

    @Override
    public DataType resultType() {
        return DataTypes.BinaryType;
    }

    public byte[] invoke(UTF8String tableName, int fieldId, long rowId) {
        if (tableName == null) {
            return null;
        }
        return new BlobReference(tableName.toString(), fieldId, rowId).serialize();
    }

    @Override
    public String name() {
        return "blob_reference";
    }
}
