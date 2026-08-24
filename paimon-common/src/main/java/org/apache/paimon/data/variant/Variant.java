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

package org.apache.paimon.data.variant;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.types.DataType;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * A Variant represents a type that contain one of: 1) Primitive: A type and corresponding value
 * (e.g. INT, STRING); 2) Array: An ordered list of Variant values; 3) Object: An unordered
 * collection of string/Variant pairs (i.e. key/value pairs). An object may not contain duplicate
 * keys.
 *
 * <p>A Variant is encoded with 2 binary values, the value and the metadata.
 *
 * <p>The Variant Binary Encoding allows representation of semi-structured data (e.g. JSON) in a
 * form that can be efficiently queried by path. The design is intended to allow efficient access to
 * nested data even in the presence of very wide or deep structures.
 */
public interface Variant {

    String METADATA = "metadata";

    String VALUE = "value";

    /** Creates a Variant from the value and metadata fields in a physical Variant row. */
    static Variant fromRow(InternalRow row) {
        if (row instanceof ColumnarRow) {
            ColumnarRow columnarRow = (ColumnarRow) row;
            return new GenericVariant(
                    columnarRow.getBinaryBuffer(0), columnarRow.getBinaryBuffer(1));
        }
        return new GenericVariant(row.getBinary(0), row.getBinary(1));
    }

    /** Returns the variant metadata. */
    byte[] metadata();

    /**
     * Returns a view of the serialized metadata without requiring a copy.
     *
     * <p>The bytes are between the returned buffer's position and limit. The buffer and its backing
     * storage may be reused by the reader, so callers that retain the bytes must copy them.
     * Implementations should return a new buffer view on each call.
     */
    ByteBuffer metadataBuffer();

    /** Returns the variant value. */
    byte[] value();

    /**
     * Returns a view of the serialized value without requiring a copy.
     *
     * <p>The bytes are between the returned buffer's position and limit. The buffer and its backing
     * storage may be reused by the reader, so callers that retain the bytes must copy them.
     * Implementations should return a new buffer view on each call.
     */
    ByteBuffer valueBuffer();

    /** Parses the variant to json. */
    default String toJson() {
        return toJson(ZoneOffset.UTC);
    }

    /** Parses the variant to json with zoneId. */
    String toJson(ZoneId zoneId);

    /**
     * Extracts a sub-variant value according to a path which start with a `$`. e.g.
     *
     * <p>access object's field: `$.key` or `$['key']` or `$["key"]`.
     *
     * <p>access array's first elem: `$.array[0]`
     *
     * <p>and then cast the value to the target type.
     */
    Object variantGet(String path, DataType dataType, VariantCastArgs castArgs);

    /** Returns the size of the variant in bytes. */
    long sizeInBytes();

    /** Returns a copy of the variant. */
    Variant copy();
}
