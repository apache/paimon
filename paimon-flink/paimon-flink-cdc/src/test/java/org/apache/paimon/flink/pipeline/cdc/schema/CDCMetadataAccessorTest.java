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

package org.apache.paimon.flink.pipeline.cdc.schema;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.cdc.common.event.TableId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CDCMetadataAccessor}. */
public class CDCMetadataAccessorTest {
    @TempDir protected static Path temporaryFolder;

    @Test
    public void test() throws Exception {
        String warehouse = temporaryFolder.toAbsolutePath().toString();

        Options options = new Options();
        options.set(CatalogOptions.WAREHOUSE, warehouse);
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("default", false);
        Identifier identifier = Identifier.create("default", "test");
        Schema schema = Schema.newBuilder().column("a", DataTypes.INT()).build();
        catalog.createTable(identifier, schema, false);

        TableId tableId = TableId.tableId("default", "test");
        CDCMetadataAccessor metadataAccessor = new CDCMetadataAccessor(catalog);
        assertThatThrownBy(metadataAccessor::listNamespaces)
                .isInstanceOf(UnsupportedOperationException.class)
                .message()
                .isEqualTo("Paimon does not support namespaces");
        assertThat(metadataAccessor.listSchemas(null)).containsExactly("default");
        assertThat(metadataAccessor.listTables(null, "default")).containsExactly(tableId);
        assertThat(metadataAccessor.listTables(null, null)).containsExactly(tableId);

        org.apache.flink.cdc.common.schema.Schema expectedSchema =
                org.apache.flink.cdc.common.schema.Schema.newBuilder()
                        .physicalColumn("a", org.apache.flink.cdc.common.types.DataTypes.INT())
                        .option("path", warehouse + "/default.db/test")
                        .build();
        org.apache.flink.cdc.common.schema.Schema actualSchema =
                metadataAccessor.getTableSchema(tableId);
        assertThat(actualSchema).isEqualTo(expectedSchema);
    }
}
