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

package org.apache.paimon.fileindex.bitmap;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;

import java.util.function.Function;

/** Utility class for bitmap index operations. */
public class BitmapIndexUtils {

    // Currently, it is mainly used to convert timestamps to long
    public static Function<Object, Object> getValueMapper(DataType dataType) {
        return dataType.accept(
                new BitmapTypeVisitor<Function<Object, Object>>() {

                    @Override
                    public Function<Object, Object> visitBinaryString() {
                        return o -> {
                            if (o instanceof BinaryString) {
                                return ((BinaryString) o).copy();
                            }
                            return o;
                        };
                    }

                    @Override
                    public Function<Object, Object> visitByte() {
                        return Function.identity();
                    }

                    @Override
                    public Function<Object, Object> visitShort() {
                        return Function.identity();
                    }

                    @Override
                    public Function<Object, Object> visitInt() {
                        return Function.identity();
                    }

                    @Override
                    public Function<Object, Object> visitLong() {
                        return Function.identity();
                    }

                    @Override
                    public Function<Object, Object> visitFloat() {
                        return Function.identity();
                    }

                    @Override
                    public Function<Object, Object> visitDouble() {
                        return Function.identity();
                    }

                    @Override
                    public Function<Object, Object> visitBoolean() {
                        return Function.identity();
                    }

                    @Override
                    public Function<Object, Object> visit(TimestampType timestampType) {
                        return getTimeStampMapper(timestampType.getPrecision());
                    }

                    @Override
                    public Function<Object, Object> visit(
                            LocalZonedTimestampType localZonedTimestampType) {
                        return getTimeStampMapper(localZonedTimestampType.getPrecision());
                    }

                    private Function<Object, Object> getTimeStampMapper(int precision) {
                        return o -> {
                            if (o == null) {
                                return null;
                            } else if (precision <= 3) {
                                return ((Timestamp) o).getMillisecond();
                            } else {
                                return ((Timestamp) o).toMicros();
                            }
                        };
                    }
                });
    }
}
