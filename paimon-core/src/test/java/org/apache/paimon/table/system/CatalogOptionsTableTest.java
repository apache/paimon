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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.TableType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;
import static org.apache.paimon.table.system.CatalogOptionsTable.CATALOG_OPTIONS;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CatalogOptionsTable}. */
public class CatalogOptionsTableTest extends TableTestBase {

    private Catalog catalog;

    private CatalogOptionsTable catalogOptionsTable;
    private Options catalogOptions;
    @TempDir java.nio.file.Path tempDir;

    @BeforeEach
    public void before() throws Exception {
        catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.TABLE_TYPE, TableType.MANAGED);
        catalogOptions.set("table-default.scan.infer-parallelism", "false");
        catalogOptions.set(CatalogOptions.WAREHOUSE, tempDir.toUri().toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
        catalogOptionsTable =
                (CatalogOptionsTable)
                        catalog.getTable(new Identifier(SYSTEM_DATABASE_NAME, CATALOG_OPTIONS));
    }

    @Test
    public void testCatalogOptionsTable() throws Exception {
        List<InternalRow> expectRow = getExpectedResult();
        List<InternalRow> result = read(catalogOptionsTable);
        assertThat(result).containsExactlyElementsOf(expectRow);
    }

    private List<InternalRow> getExpectedResult() {
        List<InternalRow> expectedRow = new ArrayList<>();
        for (Map.Entry<String, String> option : catalogOptions.toMap().entrySet()) {
            expectedRow.add(
                    GenericRow.of(
                            BinaryString.fromString(option.getKey()),
                            BinaryString.fromString(option.getValue())));
        }
        return expectedRow;
    }
}
