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

package org.apache.paimon.spark.comet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/** Builder for {@link CometColumnarBatchReader}. */
public class PaimonCometReaderBuilder {

    public static CometColumnarBatchReader build(
            StructType sparkSchema, MessageType parquetSchema) {
        List<CometColumnReader> readers = new ArrayList<>();

        for (StructField field : sparkSchema.fields()) {
            String fieldName = field.name();
            // Paimon is case-sensitive, so we strictly match the field name.
            if (parquetSchema.containsField(fieldName)) {
                Type parquetType = parquetSchema.getType(fieldName);
                if (parquetType.isPrimitive()) {
                    // Find the column descriptor
                    // TODO: Support nested types.
                    // Currently, we only support flat schemas (primitive types) to simplify the
                    // implementation.
                    // Comet supports nested types, but it requires recursive reader construction
                    // which is not yet implemented here.
                    String[] path = {fieldName};
                    ColumnDescriptor descriptor = parquetSchema.getColumnDescription(path);
                    readers.add(new CometColumnReader(field.dataType(), descriptor));
                } else {
                    throw new UnsupportedOperationException(
                            "Nested types are not supported in Paimon Comet integration yet: "
                                    + fieldName);
                }
            } else {
                // TODO: Support missing fields (ConstantColumnReader)
                throw new UnsupportedOperationException(
                        "Field missing in Parquet file: " + fieldName);
            }
        }

        return new CometColumnarBatchReader(
                readers.toArray(new CometColumnReader[0]), sparkSchema, 8192); // Default batch size
    }
}
