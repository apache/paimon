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

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.paimon.types.VarCharType.STRING_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataTypeChecks}. */
class DataTypeChecksTest {

    @Test
    void testIsCompositeTypeRowType() {
        DataType dataType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "f0", new IntType()),
                                new DataField(1, "f1", STRING_TYPE)));
        assertThat(DataTypeChecks.isCompositeType(dataType)).isTrue();
    }

    @Test
    void testIsCompositeTypeSimpleType() {
        assertThat(DataTypeChecks.isCompositeType(STRING_TYPE)).isFalse();
    }

    @Test
    void testFieldNameExtraction() {
        DataType dataType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "f0", new IntType()),
                                new DataField(1, "f1", STRING_TYPE)));
        assertThat(DataTypeChecks.getFieldNames(dataType)).containsExactly("f0", "f1");
    }

    @Test
    void testFieldCountExtraction() {
        DataType dataType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "f0", new IntType()),
                                new DataField(1, "f1", STRING_TYPE)));
        assertThat(DataTypeChecks.getFieldCount(dataType)).isEqualTo(2);
    }
}
