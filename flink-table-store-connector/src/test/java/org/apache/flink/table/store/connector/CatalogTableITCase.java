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

package org.apache.flink.table.store.connector;

import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.List;

import static org.apache.flink.table.store.file.catalog.Catalog.METADATA_TABLE_SPLITTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for catalog tables. */
public class CatalogTableITCase extends CatalogITCaseBase {

    @Test
    public void testNotExistMetadataTable() {
        assertThatThrownBy(() -> sql("SELECT snapshot_id, schema_id, commit_kind FROM T$snapshots"))
                .hasMessageContaining("Object 'T$snapshots' not found");
    }

    @Test
    public void testSnapshotsTable() throws Exception {
        sql("CREATE TABLE T (a INT, b INT)");
        sql("INSERT INTO T VALUES (1, 2)");
        sql("INSERT INTO T VALUES (3, 4)");

        List<Row> result = sql("SELECT snapshot_id, schema_id, commit_kind FROM T$snapshots");
        assertThat(result)
                .containsExactlyInAnyOrder(Row.of(1L, 0L, "APPEND"), Row.of(2L, 0L, "APPEND"));
    }

    @Test
    public void testOptionsTable() throws Exception {
        sql("CREATE TABLE T (a INT, b INT)");
        sql("ALTER TABLE T SET ('snapshot.time-retained' = '5 h')");

        List<Row> result = sql("SELECT * FROM T$options");
        assertThat(result).containsExactly(Row.of("snapshot.time-retained", "5 h"));
    }

    @Test
    public void testCreateMetaTable() {
        assertThatThrownBy(() -> sql("CREATE TABLE T$snapshots (a INT, b INT)"))
                .hasRootCauseMessage(
                        String.format(
                                "Table name[%s] cannot contain '%s' separator",
                                "T$snapshots", METADATA_TABLE_SPLITTER));
        assertThatThrownBy(() -> sql("CREATE TABLE T$aa$bb (a INT, b INT)"))
                .hasRootCauseMessage(
                        String.format(
                                "Table name[%s] cannot contain '%s' separator",
                                "T$aa$bb", METADATA_TABLE_SPLITTER));
    }
}
