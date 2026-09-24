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

package org.apache.paimon.flink;

import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for lookup changelog event metadata. */
public class LookupChangelogEventMetadataITCase extends CatalogITCaseBase {

    @Test
    public void testEventMetadataCanBeReadAsWriteTime() throws Exception {
        sql(
                "CREATE TABLE source_table ("
                        + "id INT PRIMARY KEY NOT ENFORCED, "
                        + "data INT, "
                        + "event_ts BIGINT, "
                        + "writetime BIGINT METADATA FROM '__internal__event_ts' VIRTUAL"
                        + ") WITH ("
                        + "'bucket'='1', "
                        + "'changelog-producer'='lookup', "
                        + "'sequence.field'='event_ts', "
                        + "'changelog-producer.expose-field-as-metadata'='event_ts')");

        // The metadata column models an external sink populated from an event timestamp. The
        // physical event_ts column remains available to normal Flink operators, while writetime
        // carries the incoming event value even on an UPDATE_BEFORE retraction.
        BlockingIterator<Row, Row> iterator =
                streamSqlBlockIter("SELECT id, data, event_ts, writetime FROM source_table");

        sql("INSERT INTO source_table VALUES (1, 10, 50)");
        assertThat(iterator.collect(1)).containsExactly(Row.of(1, 10, 50L, 50L));

        sql("INSERT INTO source_table VALUES (1, 20, 100)");
        assertThat(iterator.collect(2))
                .containsExactly(
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 10, 50L, 100L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 20, 100L, 100L));

        iterator.close();
    }

    @Test
    public void testPhysicalFilterStillSeesBeforeImage() throws Exception {
        sql(
                "CREATE TABLE filtered_source ("
                        + "id INT PRIMARY KEY NOT ENFORCED, "
                        + "data INT, "
                        + "event_ts BIGINT, "
                        + "writetime BIGINT METADATA FROM '__event__event_ts' VIRTUAL"
                        + ") WITH ("
                        + "'bucket'='1', "
                        + "'changelog-producer'='lookup', "
                        + "'sequence.field'='event_ts', "
                        + "'changelog-producer.metadata-field-prefix'='__event__', "
                        + "'changelog-producer.expose-field-as-metadata'='event_ts')");

        BlockingIterator<Row, Row> iterator =
                streamSqlBlockIter(
                        "SELECT id, data, event_ts, writetime "
                                + "FROM filtered_source WHERE event_ts < 75");

        sql("INSERT INTO filtered_source VALUES (1, 10, 50)");
        assertThat(iterator.collect(1)).containsExactly(Row.of(1, 10, 50L, 50L));

        // The update-after value is filtered out, but the update-before must retain the old
        // physical event_ts so that the downstream filter can retract the old row. Its metadata
        // value remains the incoming event timestamp, which is the value an external sink needs.
        sql("INSERT INTO filtered_source VALUES (1, 20, 100)");
        assertThat(iterator.collect(1))
                .containsExactly(Row.ofKind(RowKind.UPDATE_BEFORE, 1, 10, 50L, 100L));

        iterator.close();
    }

    @Test
    public void testNestedValueCanBeReadWithFlinkMetadataAlias() throws Exception {
        sql(
                "CREATE TABLE nested_source ("
                        + "id INT PRIMARY KEY NOT ENFORCED, "
                        + "event_ts BIGINT, "
                        + "payload ROW<nested INT>, "
                        + "writetime BIGINT METADATA FROM '__internal__event_ts' VIRTUAL"
                        + ") WITH ("
                        + "'bucket'='1', "
                        + "'changelog-producer'='lookup', "
                        + "'sequence.field'='event_ts', "
                        + "'changelog-producer.expose-field-as-metadata'='event_ts')");

        BlockingIterator<Row, Row> iterator =
                streamSqlBlockIter("SELECT id, event_ts, payload, writetime FROM nested_source");

        sql("INSERT INTO nested_source VALUES " + "(1, 50, CAST(ROW(10) AS ROW<nested INT>))");
        assertThat(iterator.collect(1)).containsExactly(Row.of(1, 50L, Row.of(10), 50L));

        sql("INSERT INTO nested_source VALUES " + "(1, 100, CAST(ROW(20) AS ROW<nested INT>))");
        assertThat(iterator.collect(2))
                .containsExactly(
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 50L, Row.of(10), 100L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 100L, Row.of(20), 100L));

        iterator.close();
    }

    @Test
    public void testPhysicalTableCanBeReadWithFlinkMetadataAlias() throws Exception {
        String tableName = "physical_table";
        sql(
                "CREATE TABLE "
                        + tableName
                        + " ("
                        + "id INT PRIMARY KEY NOT ENFORCED, "
                        + "data INT, "
                        + "event_ts BIGINT"
                        + ") WITH ("
                        + "'bucket'='1', "
                        + "'changelog-producer'='lookup', "
                        + "'sequence.field'='event_ts', "
                        + "'changelog-producer.expose-field-as-metadata'='event_ts')");

        // A table created without a Flink metadata alias stores only its physical columns. Verify
        // that Flink can read that schema before registering a metadata alias.
        assertThat(
                        table(tableName).getUnresolvedSchema().getColumns().stream()
                                .map(column -> column.getName())
                                .collect(Collectors.toList()))
                .containsExactly("id", "data", "event_ts");

        // Without a metadata column in the Flink schema, a wildcard sees only the three physical
        // columns and the changelog contains the same three-column shape.
        BlockingIterator<Row, Row> physicalIterator =
                streamSqlBlockIter("SELECT * FROM " + tableName);

        sql("INSERT INTO " + tableName + " VALUES (1, 10, 50)");
        assertThat(physicalIterator.collect(1)).containsExactly(Row.of(1, 10, 50L));
        assertThat(sql("SELECT * FROM " + tableName)).containsExactly(Row.of(1, 10, 50L));

        // Read the same physical table through a separate Flink connector definition. This is
        // equivalent to registering a metadata alias when consuming a table created by another
        // engine.
        sEnv.executeSql(
                String.format(
                        "CREATE TEMPORARY TABLE flink_reader ("
                                + "id INT PRIMARY KEY NOT ENFORCED, "
                                + "data INT, "
                                + "event_ts BIGINT, "
                                + "writetime BIGINT METADATA FROM '__internal__event_ts' VIRTUAL"
                                + ") WITH ("
                                + "'connector'='paimon', "
                                + "'path'='%s')",
                        getTableDirectory(tableName)));

        // A wildcard projection should include the physical columns followed by the declared
        // metadata alias, so verify the same shape while reading the changelog.
        BlockingIterator<Row, Row> iterator = streamSqlBlockIter("SELECT * FROM flink_reader");

        assertThat(iterator.collect(1)).containsExactly(Row.of(1, 10, 50L, 50L));

        sql("INSERT INTO " + tableName + " VALUES (1, 20, 100)");
        assertThat(iterator.collect(2))
                .containsExactly(
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 10, 50L, 100L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 20, 100L, 100L));

        assertThat(physicalIterator.collect(2))
                .containsExactly(
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 10, 50L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 20, 100L));

        physicalIterator.close();
        iterator.close();
    }
}
