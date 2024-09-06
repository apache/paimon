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
import org.apache.paimon.data.Decimal;

import java.util.Objects;

/**
 * Data type of a decimal number with fixed precision and scale.
 *
 * @since 0.4.0
 */
@Public
public class DecimalType extends DataType {

    private static final long serialVersionUID = 1L;

    public static final int MIN_PRECISION = 1;

    public static final int MAX_PRECISION = 38;

    public static final int DEFAULT_PRECISION = 10;

    public static final int MIN_SCALE = 0;

    public static final int DEFAULT_SCALE = 0;

    private static final String FORMAT = "DECIMAL(%d, %d)";

    private final int precision;

    private final int scale;

    public DecimalType(boolean isNullable, int precision, int scale) {
        super(isNullable, DataTypeRoot.DECIMAL);
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(
                    String.format(
                            "Decimal precision must be between %d and %d (both inclusive).",
                            MIN_PRECISION, MAX_PRECISION));
        }
        if (scale < MIN_SCALE || scale > precision) {
            throw new IllegalArgumentException(
                    String.format(
                            "Decimal scale must be between %d and the precision %d (both inclusive).",
                            MIN_SCALE, precision));
        }
        this.precision = precision;
        this.scale = scale;
    }

    public DecimalType(int precision, int scale) {
        this(true, precision, scale);
    }

    public DecimalType(int precision) {
        this(precision, DEFAULT_SCALE);
    }

    public DecimalType() {
        this(DEFAULT_PRECISION);
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public int defaultSize() {
        return Decimal.isCompact(precision) ? 8 : 16;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new DecimalType(isNullable, precision, scale);
    }

    @Override
    public String asSQLString() {
        return withNullability(FORMAT, precision, scale);
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
        DecimalType that = (DecimalType) o;
        return precision == that.precision && scale == that.scale;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision, scale);
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
