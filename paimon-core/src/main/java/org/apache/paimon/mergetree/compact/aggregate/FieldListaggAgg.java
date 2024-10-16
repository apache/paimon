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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.StringUtils;

/** listagg aggregate a field of a row. */
public class FieldListaggAgg extends FieldAggregator {

    public static final String NAME = "listagg";

    private static final long serialVersionUID = 1L;

    private final String delimiter;

    public FieldListaggAgg(DataType dataType, CoreOptions options, String field) {
        super(dataType);
        this.delimiter = options.fieldListAggDelimiter(field);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        Object concatenate;

        if (inputField == null || accumulator == null) {
            concatenate = (inputField == null) ? accumulator : inputField;
        } else {
            // ordered by type root definition
            switch (fieldType.getTypeRoot()) {
                case VARCHAR:
                    // TODO: ensure not VARCHAR(n)
                    BinaryString mergeFieldSD = (BinaryString) accumulator;
                    BinaryString inFieldSD = (BinaryString) inputField;
                    concatenate =
                            StringUtils.concat(
                                    mergeFieldSD, BinaryString.fromString(delimiter), inFieldSD);
                    break;
                default:
                    String msg =
                            String.format(
                                    "type %s not support in %s",
                                    fieldType.getTypeRoot().toString(), this.getClass().getName());
                    throw new IllegalArgumentException(msg);
            }
        }
        return concatenate;
    }
}
