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

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

package org.apache.paimon.utils;

import org.apache.paimon.types.DataType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.Function;

/**
 * Formatter to format a data to string.
 *
 * @param <T> The type of the data to format.
 */
public interface StringFormatter<T> {
    String format(T t);

    /**
     * Create a StringFormatter from a data type and a data generator.
     *
     * @param dataType The data type of the dataGenerator result.
     * @param dataGenerator The data generator to generate the data to format.
     * @param <T> The type of the data to format.
     */
    static <T> StringFormatter<T> createStringFormat(
            DataType dataType, Function<T, Object> dataGenerator) {
        switch (dataType.getTypeRoot()) {
            case BINARY:
            case VARBINARY:
                return row -> {
                    byte[] bytes = (byte[]) dataGenerator.apply(row);
                    if (bytes == null) {
                        return null;
                    }
                    return StandardCharsets.ISO_8859_1
                            .decode(Base64.getEncoder().encode(ByteBuffer.wrap(bytes)))
                            .toString();
                };
            case ARRAY:
            case ROW:
            case MULTISET:
            case MAP:
                // implement this if you need.
                throw new UnsupportedOperationException(
                        "Not support to format " + dataType + " to String currently");
            default:
                return row -> {
                    Object field = dataGenerator.apply(row);
                    if (field == null) {
                        return null;
                    }
                    return field.toString();
                };
        }
    }

    /**
     * Create a StringFormatter from a data type.
     *
     * @param dataType The data type to format.
     * @return The StringFormatter directly format the Object of data type to String.
     */
    static StringFormatter<Object> createStringFormat(DataType dataType) {
        return createStringFormat(dataType, Function.identity());
    }
}
