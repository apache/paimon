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

package org.apache.paimon.predicate;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class NullTransformTest {

    @ParameterizedTest
    @MethodSource("allTypes")
    void testAlwaysReturnNullForAllTypes(DataType type) {
        NullTransform transform = new NullTransform(new FieldRef(0, "f0", type));
        assertThat(transform.outputType()).isEqualTo(type);
        assertThat(transform.transform(GenericRow.of((Object) null))).isNull();
        assertThat(transform.transform(GenericRow.of(123))).isNull();
    }

    @Test
    void testCopyWithNewInputs() {
        FieldRef ref0 = new FieldRef(0, "f0", DataTypes.INT());
        FieldRef ref3 = new FieldRef(3, "f0", DataTypes.INT());

        NullTransform transform = new NullTransform(ref0);
        Transform copied = transform.copyWithNewInputs(Collections.singletonList(ref3));

        assertThat(copied).isEqualTo(new NullTransform(ref3));
        assertThat(copied.outputType()).isEqualTo(DataTypes.INT());
        assertThat(copied.transform(GenericRow.of((Object) null))).isNull();
    }

    private static Stream<DataType> allTypes() {
        return Stream.of(
                // numeric
                DataTypes.TINYINT(),
                DataTypes.SMALLINT(),
                DataTypes.INT(),
                DataTypes.BIGINT(),
                DataTypes.FLOAT(),
                DataTypes.DOUBLE(),
                DataTypes.DECIMAL(10, 2),

                // boolean
                DataTypes.BOOLEAN(),

                // string
                DataTypes.STRING(),
                DataTypes.CHAR(3),
                DataTypes.VARCHAR(20),

                // binary
                DataTypes.BYTES(),
                DataTypes.BINARY(8),
                DataTypes.VARBINARY(12),

                // datetime
                DataTypes.DATE(),
                DataTypes.TIME(),
                DataTypes.TIME(9),
                DataTypes.TIMESTAMP(),
                DataTypes.TIMESTAMP_MILLIS(),
                DataTypes.TIMESTAMP(9),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9),
                DataTypes.TIMESTAMP_LTZ_MILLIS(),

                // complex
                DataTypes.ARRAY(DataTypes.INT()),
                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.TIMESTAMP())),
                DataTypes.MAP(DataTypes.VARCHAR(10), DataTypes.TIMESTAMP()),
                DataTypes.MULTISET(DataTypes.STRING()),
                DataTypes.ROW(
                        DataTypes.FIELD(0, "a", DataTypes.INT()),
                        DataTypes.FIELD(1, "b", DataTypes.STRING())),

                // special
                DataTypes.VARIANT(),
                DataTypes.BLOB(),

                // not-null variants (exercise nullability flag on type)
                DataTypes.INT().copy(false),
                DataTypes.STRING().copy(false),
                DataTypes.ARRAY(DataTypes.INT()).copy(false));
    }
}
