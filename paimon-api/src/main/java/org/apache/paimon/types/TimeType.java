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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;

import java.util.Objects;

/**
 * Data type of time WITHOUT time zone consisting of {@code hour:minute:second[.fractional]}.
 * Compared to the SQL standard, leap seconds (23:59:60 and 23:59:61) are not supported as the
 * semantics are closer to {@link java.time.LocalTime}. A time WITH time zone is not provided.
 *
 * <p>Values are represented as the number of milliseconds of the day, which is why the supported
 * precision is limited to {@link #MAX_PRECISION}. A conversion from and to {@code int} describes
 * the number of milliseconds of the day.
 *
 * @since 0.4.0
 */
@Public
public final class TimeType extends DataType {

    private static final long serialVersionUID = 1L;

    public static final int MIN_PRECISION = 0;

    /**
     * Maximum supported precision. Time values are stored as milliseconds of the day in an {@code
     * int} (4 bytes) throughout the stack - the internal row representation, the row serializer and
     * the Parquet {@code TIME_MILLIS} logical type - so at most 3 fractional digits are
     * representable. A higher precision is rejected instead of being silently rounded away.
     */
    public static final int MAX_PRECISION = 3;

    public static final int DEFAULT_PRECISION = 0;

    private static final String FORMAT = "TIME(%d)";

    private final int precision;

    public TimeType(boolean isNullable, int precision) {
        super(isNullable, DataTypeRoot.TIME_WITHOUT_TIME_ZONE);
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(
                    String.format(
                            "Time precision must be between %d and %d (both inclusive).",
                            MIN_PRECISION, MAX_PRECISION));
        }
        this.precision = precision;
    }

    public TimeType(int precision) {
        this(true, precision);
    }

    public TimeType() {
        this(DEFAULT_PRECISION);
    }

    public int getPrecision() {
        return precision;
    }

    @Override
    public int defaultSize() {
        return 4;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new TimeType(isNullable, precision);
    }

    @Override
    public String asSQLString() {
        return withNullability(FORMAT, precision);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        TimeType timeType = (TimeType) o;
        return precision == timeType.precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision);
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
