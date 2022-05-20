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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test case for append-only managed table. */
public class AppendOnlyTableTest extends FileStoreTableITCase {

    @Test
    public void testCreateTableWithPrimaryKey() {
        assertThatThrownBy(
                        () ->
                                batchSql(
                                        "CREATE TABLE pk_table (id INT PRIMARY KEY NOT ENFORCED, data STRING) "
                                                + "WITH ('write-mode'='append-only')"))
                .hasRootCauseInstanceOf(TableException.class)
                .hasRootCauseMessage(
                        "Cannot define any primary key in an append-only table. Set 'write-mode'='change-log' if still "
                                + "want to keep the primary key definition.");
    }

    @Test
    public void testReadWrite() {
        batchSql("INSERT INTO append_table VALUES (1, 'AAA'), (2, 'BBB')");

        List<Row> rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1, "AAA"), Row.of(2, "BBB"));

        rows = batchSql("SELECT id FROM append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1), Row.of(2));

        rows = batchSql("SELECT data from append_table");
        assertThat(rows.size()).isEqualTo(2);
        assertThat(rows).containsExactlyInAnyOrder(Row.of("AAA"), Row.of("BBB"));
    }

    @Test
    public void testSkipDedup() {
        batchSql("INSERT INTO append_table VALUES (1, 'AAA'), (1, 'AAA'), (2, 'BBB'), (3, 'BBB')");

        List<Row> rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, "AAA"), Row.of(1, "AAA"), Row.of(2, "BBB"), Row.of(3, "BBB"));

        rows = batchSql("SELECT id FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1), Row.of(1), Row.of(2), Row.of(3));

        rows = batchSql("SELECT data FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("AAA"), Row.of("AAA"), Row.of("BBB"), Row.of("BBB"));
    }

    @Test
    public void testIngestFromSource() {
        List<Row> input =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, "AAA"),
                        Row.ofKind(RowKind.INSERT, 1, "AAA"),
                        Row.ofKind(RowKind.INSERT, 1, "BBB"),
                        Row.ofKind(RowKind.INSERT, 2, "AAA"));

        String id = TestValuesTableFactory.registerData(input);
        batchSql(
                "CREATE TABLE source (id INT, data STRING) WITH ('connector'='values', 'bounded'='true', 'data-id'='%s')",
                id);

        batchSql("INSERT INTO append_table SELECT * FROM source");

        List<Row> rows = batchSql("SELECT * FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, "AAA"), Row.of(1, "AAA"), Row.of(1, "BBB"), Row.of(2, "AAA"));

        rows = batchSql("SELECT id FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1), Row.of(1), Row.of(1), Row.of(2));

        rows = batchSql("SELECT data FROM append_table");
        assertThat(rows.size()).isEqualTo(4);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("AAA"), Row.of("AAA"), Row.of("BBB"), Row.of("AAA"));
    }

    private void testRejectChanges(RowKind kind) {
        List<Row> input = Collections.singletonList(Row.ofKind(kind, 1, "AAA"));

        String id = TestValuesTableFactory.registerData(input);
        batchSql(
                "CREATE TABLE source (id INT, data STRING) WITH ('connector'='values', 'bounded'='true', 'data-id'='%s')",
                id);

        assertThatThrownBy(() -> batchSql("INSERT INTO append_table SELECT * FROM source"))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("Append only writer can not accept row with RowKind %s", kind);
    }

    @Test
    public void testRejectDelete() {
        testRejectChanges(RowKind.DELETE);
    }

    @Test
    public void testRejectUpdateBefore() {
        testRejectChanges(RowKind.UPDATE_BEFORE);
    }

    @Test
    public void testRejectUpdateAfter() {
        testRejectChanges(RowKind.UPDATE_BEFORE);
    }

    @Override
    protected List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE IF NOT EXISTS append_table (id INT, data STRING) WITH ('write-mode'='append-only')");
    }
}
