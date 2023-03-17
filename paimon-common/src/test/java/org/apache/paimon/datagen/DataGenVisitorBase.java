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

package org.apache.paimon.datagen;

import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.TimeType;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.function.Supplier;

import static java.time.temporal.ChronoField.MILLI_OF_DAY;

/** Base class for translating {@link DataType} to {@link DataGeneratorContainer}'s. */
public abstract class DataGenVisitorBase extends DataTypeDefaultVisitor<DataGeneratorContainer> {

    protected final String name;

    protected final Options config;

    protected DataGenVisitorBase(String name, Options config) {
        this.name = name;
        this.config = config;
    }

    @Override
    public DataGeneratorContainer visit(DateType dateType) {
        return DataGeneratorContainer.of(
                TimeGenerator.of(() -> (int) LocalDate.now().toEpochDay()));
    }

    @Override
    public DataGeneratorContainer visit(TimeType timeType) {
        return DataGeneratorContainer.of(TimeGenerator.of(() -> LocalTime.now().get(MILLI_OF_DAY)));
    }

    @Override
    protected DataGeneratorContainer defaultMethod(DataType dataType) {
        throw new RuntimeException("Unsupported type: " + dataType);
    }

    private interface SerializableSupplier<T> extends Supplier<T>, Serializable {}

    private abstract static class TimeGenerator<T> implements DataGenerator<T> {

        public static <T> TimeGenerator<T> of(SerializableSupplier<T> supplier) {
            return new TimeGenerator<T>() {
                @Override
                public T next() {
                    return supplier.get();
                }
            };
        }

        @Override
        public void open() {}

        @Override
        public boolean hasNext() {
            return true;
        }
    }
}
