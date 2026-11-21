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

import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests the build-schema checks for BTree payloads. */
class BTreeIndexSchemaTest {

    @Test
    void testLegacyIndexRequiresAnUnchangedKeyType() throws Exception {
        RowType old = RowType.of(DataTypes.INT());
        RowType widened = RowType.of(DataTypes.BIGINT());
        IndexFileMeta legacy =
                index("btree-global-index-00000000-0000-0000-0000-000000000001.index");
        assertThat(new BTreeIndexSchema(table(old, old)).isCompatible(legacy)).isTrue();
        assertThat(new BTreeIndexSchema(table(old, widened)).isCompatible(legacy)).isFalse();
    }

    @Test
    void testTaggedIndexOnlyComparesTheBuildSchema() throws Exception {
        RowType old = RowType.of(DataTypes.INT());
        RowType widened = RowType.of(DataTypes.BIGINT());
        BTreeIndexSchema schema = new BTreeIndexSchema(table(old, widened));
        assertThat(
                        schema.isCompatible(
                                index(
                                        "btree-global-index-schema-0-00000000-0000-0000-0000-000000000001.index")))
                .isFalse();
        assertThat(
                        schema.isCompatible(
                                index(
                                        "btree-global-index-schema-1-00000000-0000-0000-0000-000000000001.index")))
                .isTrue();
    }

    private static FileStoreTable table(RowType before, RowType current) throws Exception {
        FileStoreTable table = mock(FileStoreTable.class);
        SchemaManager manager = mock(SchemaManager.class);
        TableSchema beforeSchema = mock(TableSchema.class);
        TableSchema currentSchema = mock(TableSchema.class);
        when(beforeSchema.logicalRowType()).thenReturn(before);
        when(currentSchema.id()).thenReturn(1L);
        when(currentSchema.logicalRowType()).thenReturn(current);
        when(manager.tryGetSchema(0)).thenReturn(beforeSchema);
        when(table.schemaManager()).thenReturn(manager);
        when(table.schema()).thenReturn(currentSchema);
        when(table.rowType()).thenReturn(current);
        return table;
    }

    private static IndexFileMeta index(String name) {
        return new IndexFileMeta(
                "btree", name, 1, 1, new GlobalIndexMeta(0, 0, 0, null, null), null);
    }
}
