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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.JsonSerdeUtil.toFlatJson;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link SchemasTable}. */
public class SchemasTableTest extends TableTestBase {

    private SchemasTable schemasTable;
    private SchemaManager schemaManager;

    @BeforeEach
    public void before() throws Exception {
        Identifier identifier = identifier("T");
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "input")
                        .build();
        catalog.createTable(identifier, schema, true);
        schemasTable = (SchemasTable) catalog.getTable(identifier("T$schemas"));

        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, "T"));
        schemaManager = new SchemaManager(fileIO, tablePath);
    }

    @Test
    public void testSchemasTable() throws Exception {
        List<InternalRow> expectRow = getExpectedResult();
        List<InternalRow> result = read(schemasTable);
        assertThat(result).containsExactlyElementsOf(expectRow);
    }

    private List<InternalRow> getExpectedResult() {
        List<TableSchema> tableSchemas = schemaManager.listAll();

        List<InternalRow> expectedRow = new ArrayList<>();
        for (TableSchema schema : tableSchemas) {
            expectedRow.add(
                    GenericRow.of(
                            schema.id(),
                            BinaryString.fromString(toFlatJson(schema.fields())),
                            BinaryString.fromString(toFlatJson(schema.partitionKeys())),
                            BinaryString.fromString(toFlatJson(schema.primaryKeys())),
                            BinaryString.fromString(toFlatJson(schema.options())),
                            BinaryString.fromString(schema.comment()),
                            Timestamp.fromLocalDateTime(
                                    LocalDateTime.ofInstant(
                                            Instant.ofEpochMilli(schema.timeMillis()),
                                            ZoneId.systemDefault()))));
        }
        return expectedRow;
    }
}
