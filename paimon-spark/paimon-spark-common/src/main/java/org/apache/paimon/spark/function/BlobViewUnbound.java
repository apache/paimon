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
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;

/** Function unbound to {@link BlobViewSparkFunction}. */
public class BlobViewUnbound implements UnboundFunction {

    @Override
    public BoundFunction bind(StructType inputType) {
        if (inputType.fields().length != 3) {
            throw new UnsupportedOperationException(
                    "Function 'blob_view' requires 3 arguments (tableName STRING, fieldId INT, rowId BIGINT), but found "
                            + inputType.fields().length);
        }
        if (!(inputType.fields()[0].dataType() instanceof StringType)) {
            throw new UnsupportedOperationException(
                    "The first argument of 'blob_view' must be STRING type.");
        }
        if (!(inputType.fields()[1].dataType() instanceof IntegerType)) {
            throw new UnsupportedOperationException(
                    "The second argument of 'blob_view' must be INT type.");
        }
        if (!(inputType.fields()[2].dataType() instanceof LongType)) {
            throw new UnsupportedOperationException(
                    "The third argument of 'blob_view' must be BIGINT type.");
        }
        return new BlobViewSparkFunction();
    }

    @Override
    public String description() {
        return "Construct a serialized BlobViewStruct from tableName, fieldId and rowId";
    }

    @Override
    public String name() {
        return "blob_view";
    }
}
