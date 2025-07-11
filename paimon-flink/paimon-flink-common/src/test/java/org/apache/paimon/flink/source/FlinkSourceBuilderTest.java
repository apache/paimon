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

package org.apache.paimon.flink.source;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link FlinkSourceBuilder}. */
public class FlinkSourceBuilderTest {

    @TempDir Path tempDir;
    private Catalog catalog;

    @BeforeEach
    public void setUp() {
        try {
            initCatalog();
        } catch (Exception e) {
            throw new RuntimeException("Catalog initialization failed", e);
        }
    }

    private void initCatalog() throws Exception {
        if (catalog == null) {
            catalog =
                    CatalogFactory.createCatalog(
                            CatalogContext.create(new org.apache.paimon.fs.Path(tempDir.toUri())));
            catalog.createDatabase("default", false);
        }
    }

    private Table createTable(
            String tableName, boolean hasPrimaryKey, int bucketNum, boolean bucketAppendOrdered)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .option("bucket", bucketNum + "")
                        .option("bucket-append-ordered", String.valueOf(bucketAppendOrdered));

        if (hasPrimaryKey) {
            schemaBuilder.primaryKey("a");
        }

        if (bucketNum != -1) {
            schemaBuilder.option("bucket-key", "a");
        }

        Schema schema = schemaBuilder.build();
        Identifier identifier = Identifier.create("default", tableName);
        catalog.createTable(identifier, schema, false);
        return catalog.getTable(identifier);
    }

    @Test
    public void testUnawareBucket() throws Exception {
        // pk table && bucket-append-ordered is true
        Table table = createTable("t1", true, 2, true);
        FlinkSourceBuilder builder = new FlinkSourceBuilder(table);
        assertFalse(builder.isUnordered());

        // pk table && bucket-append-ordered is false
        table = createTable("t2", true, 2, false);
        builder = new FlinkSourceBuilder(table);
        assertFalse(builder.isUnordered());

        // pk table && bucket num == -1 && bucket-append-ordered is false
        table = createTable("t3", true, -1, false);
        builder = new FlinkSourceBuilder(table);
        assertFalse(builder.isUnordered());

        // append table && bucket num != 1 && bucket-append-ordered is true
        table = createTable("t4", false, 2, true);
        builder = new FlinkSourceBuilder(table);
        assertFalse(builder.isUnordered());

        // append table && bucket num == -1
        table = createTable("t5", false, -1, true);
        builder = new FlinkSourceBuilder(table);
        assertTrue(builder.isUnordered());

        // append table && bucket-append-ordered is false
        table = createTable("t6", false, 2, false);
        builder = new FlinkSourceBuilder(table);
        assertTrue(builder.isUnordered());
    }
}
