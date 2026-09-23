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

package org.apache.paimon.schema;

import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SchemaChange}. */
public class SchemaChangeTest {

    private static String[] fieldNames() {
        return new String[] {"outer", "inner"};
    }

    private static void assertEqualHashCode(SchemaChange a, SchemaChange b) {
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void testAddColumnHashCodeUsesFieldNamesContent() {
        assertEqualHashCode(
                SchemaChange.addColumn(fieldNames(), DataTypes.INT(), "c", null),
                SchemaChange.addColumn(fieldNames(), DataTypes.INT(), "c", null));
    }

    @Test
    void testRenameColumnHashCodeUsesFieldNamesContent() {
        assertEqualHashCode(
                SchemaChange.renameColumn(fieldNames(), "newName"),
                SchemaChange.renameColumn(fieldNames(), "newName"));
    }

    @Test
    void testDropColumnHashCodeUsesFieldNamesContent() {
        assertEqualHashCode(
                SchemaChange.dropColumn(fieldNames()), SchemaChange.dropColumn(fieldNames()));
    }

    @Test
    void testUpdateColumnTypeHashCodeUsesFieldNamesContent() {
        assertEqualHashCode(
                SchemaChange.updateColumnType(fieldNames(), DataTypes.BIGINT(), false),
                SchemaChange.updateColumnType(fieldNames(), DataTypes.BIGINT(), false));
    }

    @Test
    void testUpdateColumnDefaultValueHashCodeUsesFieldNamesContent() {
        assertEqualHashCode(
                SchemaChange.updateColumnDefaultValue(fieldNames(), "1"),
                SchemaChange.updateColumnDefaultValue(fieldNames(), "1"));
    }
}
