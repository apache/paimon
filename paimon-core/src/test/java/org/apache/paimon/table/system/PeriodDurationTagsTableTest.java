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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link TagsTable}. */
public class PeriodDurationTagsTableTest extends TableTestBase {

    private static final String tableName = "3TagTestTable";
    private TagsTable tagsTable;
    private FileStoreTable table;

    @BeforeEach
    void before() throws Exception {
        Identifier identifier = identifier(tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("product_id", DataTypes.INT())
                        .column("price", DataTypes.INT())
                        .column("sales", DataTypes.INT())
                        .primaryKey("product_id")
                        .option("tag.automatic-creation", "watermark")
                        .option("tag.creation-period-duration", "120 s")
                        .build();
        catalog.createTable(identifier, schema, true);
        table = (FileStoreTable) catalog.getTable(identifier);
        TableCommitImpl commit = table.newCommit(commitUser).ignoreEmptyCommit(false);

        commit.commit(
                new ManifestCommittable(
                        0,
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2025-01-07T12:00:01"))
                                .getMillisecond()));
        commit.commit(
                new ManifestCommittable(
                        1,
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2025-01-07T15:00:01"))
                                .getMillisecond()));

        tagsTable = (TagsTable) catalog.getTable(identifier(tableName + "$tags"));
    }

    @Test
    void testCreateTagsWithCustomDuration() throws Exception {
        List<InternalRow> result = read(tagsTable);
        assertThat(result.size()).isEqualTo(2);
        assertThat(result.get(0).getString(0).toString()).isEqualTo("202501071158");
        assertThat(result.get(1).getString(0).toString()).isEqualTo("202501071458");
    }

    @Override
    public void after() throws IOException {
        table.deleteTag("202501071158");
        table.deleteTag("202501071458");
    }
}
