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

package org.apache.paimon.table;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for data type cast error message contains column name. */
public class DataTypeCastTableTest extends TableTestBase {

    @Test
    public void testStringToIntCastErrorMessageContainsColumnName() throws Exception {
        // Create table with STRING column
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("str_col", DataTypes.STRING())
                        .primaryKey("pk")
                        .option("bucket", "1")
                        .build();
        catalog.createTable(identifier("CastTestTable"), schema, true);

        // Write data with non-numeric string that cannot be cast to INT
        FileStoreTable table = getTable(identifier("CastTestTable"));
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("not_a_number")),
                GenericRow.of(2, BinaryString.fromString("123")),
                GenericRow.of(3, BinaryString.fromString("invalid_value")));

        // Alter table: change column type from STRING to INT
        catalog.alterTable(
                identifier("CastTestTable"),
                Collections.singletonList(
                        SchemaChange.updateColumnType("str_col", DataTypes.INT())),
                false);

        // Read the table, should throw exception with column name in message
        FileStoreTable alteredTable = getTable(identifier("CastTestTable"));
        assertThatThrownBy(() -> read(alteredTable))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to cast value for field 'str_col'");
    }

    @Test
    public void testStringToIntCastSuccess() throws Exception {
        // Create table with STRING column
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("str_col", DataTypes.STRING())
                        .primaryKey("pk")
                        .option("bucket", "1")
                        .build();
        catalog.createTable(identifier("CastSuccessTable"), schema, true);

        // Write data with numeric strings that can be cast to INT
        FileStoreTable table = getTable(identifier("CastSuccessTable"));
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("100")),
                GenericRow.of(2, BinaryString.fromString("200")),
                GenericRow.of(3, BinaryString.fromString("300")));

        // Alter table: change column type from STRING to INT
        catalog.alterTable(
                identifier("CastSuccessTable"),
                Collections.singletonList(
                        SchemaChange.updateColumnType("str_col", DataTypes.INT())),
                false);

        // Read should succeed
        FileStoreTable alteredTable = getTable(identifier("CastSuccessTable"));
        read(alteredTable);
    }
}
