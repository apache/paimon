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
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.table.system.AggregationTable.extractFieldMap;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link AggregationTable}. */
public class AggregationTableTest extends TableTestBase {

    private AggregationTable aggregationTable;
    private SchemaManager schemaManager;

    @BeforeEach
    public void before() throws Exception {
        Identifier identifier = identifier("T");
        Schema schema =
                Schema.newBuilder()
                        .column("product_id", DataTypes.INT())
                        .column("price", DataTypes.INT())
                        .column("sales", DataTypes.INT())
                        .primaryKey("product_id")
                        .option("merge-engine", "aggregation")
                        .option("fields.price.aggregate-function", "max")
                        .option("fields.sales.aggregate-function", "sum")
                        .option(CoreOptions.AGG_FUNCTION, "input")
                        .build();
        catalog.createTable(identifier, schema, true);
        aggregationTable = (AggregationTable) catalog.getTable(identifier("T$aggregation"));

        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, "T"));
        schemaManager = new SchemaManager(fileIO, tablePath);
    }

    @Test
    public void testSchemasTable() throws Exception {
        List<InternalRow> expectRow = getExceptedResult();
        List<InternalRow> result = read(aggregationTable);
        assertThat(result).containsExactlyElementsOf(expectRow);
    }

    private List<InternalRow> getExceptedResult() {
        TableSchema schema = schemaManager.latest().get();
        Map<String, String> fieldMap = extractFieldMap(schema.options());

        GenericRow genericRow;
        List<InternalRow> expectedRow = new ArrayList<>();
        for (int i = 0; i < schema.fields().size(); i++) {
            String fieldName = schema.fields().get(i).name();
            genericRow =
                    GenericRow.of(
                            BinaryString.fromString(fieldName),
                            BinaryString.fromString(schema.fields().get(i).type().toString()),
                            BinaryString.fromString(fieldMap.get(fieldName)),
                            BinaryString.fromString(schema.fields().get(i).description()));
            expectedRow.add(genericRow);
        }
        return expectedRow;
    }
}
