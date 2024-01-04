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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link OptionsTableTest}. */
public class OptionsTableTest extends TableTestBase {

    private static final String tableName = "MyTable";
    private OptionsTable optionsTable;
    private SchemaManager schemaManager;

    @BeforeEach
    public void before() throws Exception {
        Identifier identifier = identifier(tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("product_id", DataTypes.INT())
                        .column("price", DataTypes.INT())
                        .column("sales", DataTypes.INT())
                        .primaryKey("product_id")
                        .option(CoreOptions.MERGE_ENGINE.key(), "deduplicate")
                        .option(CoreOptions.TAG_AUTOMATIC_CREATION.key(), "watermark")
                        .option(CoreOptions.TAG_CREATION_PERIOD.key(), "daily")
                        .build();
        catalog.createTable(identifier, schema, true);
        optionsTable = (OptionsTable) catalog.getTable(identifier(tableName + "$options"));

        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, tableName));
        schemaManager = new SchemaManager(fileIO, tablePath);
    }

    @Test
    public void testOptionsTable() throws Exception {
        List<InternalRow> expectRow = getExceptedResult();
        List<InternalRow> result = read(optionsTable);
        assertThat(result).containsExactlyElementsOf(expectRow);
    }

    private List<InternalRow> getExceptedResult() {
        Map<String, String> options =
                schemaManager
                        .latest()
                        .orElseThrow(() -> new RuntimeException("Table does not exist."))
                        .options();

        List<InternalRow> expectedRows = new ArrayList<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            GenericRow genericRow =
                    GenericRow.of(
                            BinaryString.fromString(entry.getKey()),
                            BinaryString.fromString(entry.getValue()));
            expectedRows.add(genericRow);
        }
        return expectedRows;
    }
}
