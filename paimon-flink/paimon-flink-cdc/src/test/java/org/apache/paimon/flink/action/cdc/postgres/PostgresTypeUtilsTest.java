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

package org.apache.paimon.flink.action.cdc.postgres;

import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PostgresTypeUtils#toDataType}. */
public class PostgresTypeUtilsTest {

    private static final TypeMapping EMPTY = TypeMapping.defaultMapping();

    /**
     * Postgres {@code numeric} allows precision up to 1000, which exceeds Paimon {@link
     * DecimalType#MAX_PRECISION} (38). Out-of-range precision must fall back to {@code STRING}
     * instead of throwing, mirroring {@code MySqlTypeUtils}.
     */
    @Test
    public void testNumericPrecisionOutOfRangeMapsToString() {
        assertThat(PostgresTypeUtils.toDataType("numeric", 50, 2, EMPTY))
                .isEqualTo(DataTypes.STRING());
    }

    /** In-range precision keeps its {@code DECIMAL(precision, scale)} mapping. */
    @Test
    public void testNumericInRangeMapsToDecimal() {
        assertThat(PostgresTypeUtils.toDataType("numeric", 10, 2, EMPTY))
                .isEqualTo(DataTypes.DECIMAL(10, 2));
        assertThat(PostgresTypeUtils.toDataType("numeric", DecimalType.MAX_PRECISION, 0, EMPTY))
                .isEqualTo(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 0));
    }

    /** Unspecified precision (0) preserves the default {@code DECIMAL(38, 18)} mapping. */
    @Test
    public void testNumericWithoutPrecisionMapsToDefaultDecimal() {
        assertThat(PostgresTypeUtils.toDataType("numeric", 0, 0, EMPTY))
                .isEqualTo(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 18));
    }

    /** The {@code _numeric} array type mirrors the scalar behaviour element-wise. */
    @Test
    public void testNumericArrayMirrorsScalarMapping() {
        assertThat(PostgresTypeUtils.toDataType("_numeric", 50, 2, EMPTY))
                .isEqualTo(DataTypes.ARRAY(DataTypes.STRING()));
        assertThat(PostgresTypeUtils.toDataType("_numeric", 10, 2, EMPTY))
                .isEqualTo(DataTypes.ARRAY(DataTypes.DECIMAL(10, 2)));
        assertThat(PostgresTypeUtils.toDataType("_numeric", 0, 0, EMPTY))
                .isEqualTo(DataTypes.ARRAY(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 18)));
    }
}
