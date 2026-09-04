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

package org.apache.paimon.globalindex;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GlobalIndexSchemaCompatibility}. */
public class GlobalIndexSchemaCompatibilityTest extends TableTestBase {

    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("indexed", DataTypes.INT())
                .option(CoreOptions.BUCKET.key(), "-1")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .build();
    }

    @Test
    public void testCompatibilityUsesIndexedFieldTypes() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        long buildSchemaId = table.schema().id();
        IndexManifestEntry entry = globalIndexEntry(buildSchemaId);

        catalog.alterTable(
                identifier(),
                Collections.singletonList(SchemaChange.addColumn("unrelated", DataTypes.STRING())),
                false);
        table = getTableDefault();
        assertThat(
                        GlobalIndexSchemaCompatibility.filterCompatible(
                                table, Collections.singleton(entry)))
                .containsExactly(entry);

        catalog.alterTable(
                identifier(),
                Collections.singletonList(
                        SchemaChange.updateColumnType("indexed", DataTypes.BIGINT())),
                false);
        table = getTableDefault();
        assertThat(
                        GlobalIndexSchemaCompatibility.filterCompatible(
                                table, Collections.singleton(entry)))
                .isEmpty();
    }

    @Test
    public void testMissingSchemaIdentityFailsClosed() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        assertThat(
                        GlobalIndexSchemaCompatibility.filterCompatible(
                                table,
                                Arrays.asList(
                                        globalIndexEntry(null), globalIndexEntry(Long.MAX_VALUE))))
                .isEmpty();
    }

    private static IndexManifestEntry globalIndexEntry(Long schemaId) {
        IndexFileMeta indexFile =
                new IndexFileMeta(
                        "btree",
                        "index-file",
                        1L,
                        1L,
                        null,
                        null,
                        new GlobalIndexMeta(0L, 0L, 0, null, null),
                        schemaId);
        return new IndexManifestEntry(FileKind.ADD, BinaryRow.EMPTY_ROW, 0, indexFile, schemaId);
    }
}
