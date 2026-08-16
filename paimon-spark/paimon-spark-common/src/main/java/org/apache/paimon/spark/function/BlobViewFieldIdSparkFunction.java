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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BlobViewStruct;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;

/** Spark scalar function that constructs a serialized {@link BlobViewStruct} by field id. */
public class BlobViewFieldIdSparkFunction implements ScalarFunction<byte[]>, Serializable {

    @Override
    public DataType[] inputTypes() {
        return new DataType[] {DataTypes.StringType, DataTypes.IntegerType, DataTypes.LongType};
    }

    @Override
    public DataType resultType() {
        return DataTypes.BinaryType;
    }

    public byte[] invoke(UTF8String identifier, int fieldId, long rowId) {
        if (identifier == null) {
            return null;
        }
        return blobViewStruct(identifier, fieldId, rowId);
    }

    @Override
    public byte[] produceResult(InternalRow input) {
        if (input.isNullAt(0) || input.isNullAt(1) || input.isNullAt(2)) {
            return null;
        }
        return blobViewStruct(input.getUTF8String(0), input.getInt(1), input.getLong(2));
    }

    private byte[] blobViewStruct(UTF8String identifier, int fieldId, long rowId) {
        return new BlobViewStruct(Identifier.fromString(identifier.toString()), fieldId, rowId)
                .serialize();
    }

    @Override
    public String name() {
        return "blob_view";
    }
}
