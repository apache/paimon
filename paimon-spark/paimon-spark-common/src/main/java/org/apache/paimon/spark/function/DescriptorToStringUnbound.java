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
import org.apache.spark.sql.types.StructType;

/** Function unbound to {@link PathToDescriptorFunction}. */
public class DescriptorToStringUnbound implements UnboundFunction {

    @Override
    public BoundFunction bind(StructType inputType) {
        if (inputType.fields().length != 1) {
            throw new UnsupportedOperationException(
                    "Function 'descriptor_to_string' requires 1 argument, but found "
                            + inputType.fields().length);
        }

        if (!(inputType.fields()[0].dataType() instanceof BinaryType)) {
            throw new UnsupportedOperationException(
                    "The first argument of 'descriptor_to_string' must be BINARY type, but found "
                            + inputType.fields()[0].dataType().simpleString());
        }

        return new PathToDescriptorFunction();
    }

    @Override
    public String description() {
        return "Convert file path to blob descriptor";
    }

    @Override
    public String name() {
        return "descriptor_to_string";
    }
}
