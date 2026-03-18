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

import org.apache.paimon.data.BlobDescriptor;

import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;

/** Function to convert blob descriptor to its string representation. */
public class DescriptorToStringFunction implements ScalarFunction<UTF8String>, Serializable {

    @Override
    public DataType[] inputTypes() {
        return new DataType[] {DataTypes.BinaryType};
    }

    @Override
    public DataType resultType() {
        return DataTypes.StringType;
    }

    public UTF8String invoke(byte[] descriptorBytes) {
        if (descriptorBytes == null) {
            return null;
        }

        BlobDescriptor descriptor = BlobDescriptor.deserialize(descriptorBytes);
        return UTF8String.fromString(descriptor.toString());
    }

    @Override
    public String name() {
        return "descriptor_to_string";
    }
}
