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

    /** Returns the variant metadata. */
    byte[] metadata();

    /** Returns the variant value. */
    byte[] value();

    /** Parses the variant to json. */
    String toJson();

    /**
     * Extracts a sub-variant value according to a path which start with a `$`. e.g.
     *
     * <p>access object's field: `$.key` or `$['key']` or `$["key"]`.
     *
     * <p>access array's first elem: `$.array[0]`
     */
    Object variantGet(String path);
}
