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

package org.apache.flink.types.variant;

/**
 * A data structure that represents a semi-structured value. It consists of two binary values: value
 * and metadata. The value encodes types and values, but not field names. The metadata currently
 * contains a version flag and a list of field names. We can extend/modify the detailed binary
 * format given the version flag.
 *
 * @see <a href="https://github.com/apache/parquet-format/blob/master/VariantEncoding.md">Variant
 *     Binary Encoding</a> for the detail layout of the data structure.
 */
public class BinaryVariant implements Variant {
    public BinaryVariant(byte[] value, byte[] metadata) {}

    public byte[] getValue() {
        throw new UnsupportedOperationException();
    }

    public byte[] getMetadata() {
        throw new UnsupportedOperationException();
    }
}
