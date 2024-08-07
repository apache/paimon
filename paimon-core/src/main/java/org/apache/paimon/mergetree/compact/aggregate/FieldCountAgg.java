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

import org.apache.paimon.types.DataType;

/** count value aggregate a field of a row. */
public class FieldCountAgg extends FieldAggregator {

    public static final String NAME = "count";

    public FieldCountAgg(DataType dataType) {
        super(dataType);
    }

    @Override
    String name() {
        return NAME;
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {

        if (accumulator != null && inputField == null) {
            return accumulator;
        }
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case TINYINT:
                return accumulator == null
                        ? (inputField == null ? (byte) 0 : (byte) 1)
                        : (byte) ((byte) accumulator + 1);
            case SMALLINT:
                return accumulator == null
                        ? (inputField == null ? (short) 0 : (short) 1)
                        : (short) ((short) accumulator + 1);
            case INTEGER:
                return accumulator == null ? (inputField == null ? 0 : 1) : (int) accumulator + 1;
            case BIGINT:
                return accumulator == null
                        ? (inputField == null ? 0L : 1L)
                        : (long) accumulator + 1L;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                fieldType.getTypeRoot().toString(), this.getClass().getName());
                throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public Object retract(Object accumulator, Object inputField) {

        if (accumulator != null && inputField == null) {
            return accumulator;
        }

        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case TINYINT:
                return accumulator == null
                        ? (inputField == null ? (byte) 0 : (byte) -1)
                        : (byte) ((byte) accumulator - 1);
            case SMALLINT:
                return accumulator == null
                        ? (inputField == null ? (short) 0 : (short) -1)
                        : (short) ((short) accumulator - 1);
            case INTEGER:
                return accumulator == null ? (inputField == null ? 0 : -1) : (int) accumulator - 1;

            case BIGINT:
                return accumulator == null
                        ? (inputField == null ? 0L : -1L)
                        : (long) accumulator - 1L;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                fieldType.getTypeRoot().toString(), this.getClass().getName());
                throw new IllegalArgumentException(msg);
        }
    }
}
