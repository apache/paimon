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

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for schema changes. */
public class SchemaChangeITCase extends CatalogITCaseBase {
    @Test
    public void testSetAndRemoveOption() throws Exception {
        sql("CREATE TABLE T (a STRING, b STRING, c STRING)");
        sql("ALTER TABLE T SET ('xyc'='unknown1', 'abc'='unknown2')");

        Map<String, String> options = table("T").getOptions();
        assertThat(options).containsEntry("xyc", "unknown1");
        assertThat(options).containsEntry("abc", "unknown2");

        sql("ALTER TABLE T RESET ('xyc', 'abc')");

        options = table("T").getOptions();
        assertThat(options).doesNotContainKey("xyc");
        assertThat(options).doesNotContainKey("abc");
    }

    @Test
    public void testSetAndResetImmutableOptionsOnEmptyTables() {
        sql("CREATE TABLE T1 (a INT, b INT)");
        sql(
                "ALTER TABLE T1 SET ('primary-key' = 'a', 'bucket' = '1', 'merge-engine' = 'first-row')");
        sql("INSERT INTO T1 VALUES (1, 10), (2, 20), (1, 11), (2, 21)");
        assertThat(queryAndSort("SELECT * FROM T1")).containsExactly(Row.of(1, 10), Row.of(2, 20));
        assertThatThrownBy(() -> sql("ALTER TABLE T1 SET ('merge-engine' = 'deduplicate')"))
                .rootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Change 'merge-engine' is not supported yet.");

        sql(
                "CREATE TABLE T2 (a INT, b INT, PRIMARY KEY (a) NOT ENFORCED) WITH ('bucket' = '1', 'merge-engine' = 'first-row')");
        sql("ALTER TABLE T2 RESET ('merge-engine')");
        sql("INSERT INTO T2 VALUES (1, 10), (2, 20), (1, 11), (2, 21)");
        assertThat(queryAndSort("SELECT * FROM T2")).containsExactly(Row.of(1, 11), Row.of(2, 21));
    }

    @Test
    public void testSetAndResetImmutableOptionsOnNonEmptyTables() {
        // bucket-key is immutable
        sql("CREATE TABLE T1 (a STRING, b STRING, c STRING)");
        sql("INSERT INTO T1 VALUES ('a', 'b', 'c')");

        assertThatThrownBy(() -> sql("ALTER TABLE T1 SET ('bucket-key' = 'c')"))
                .rootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Change 'bucket-key' is not supported yet.");

        sql(
                "CREATE TABLE T2 (a STRING, b STRING, c STRING) WITH ('bucket' = '1', 'bucket-key' = 'c')");
        sql("INSERT INTO T2 VALUES ('a', 'b', 'c')");
        assertThatThrownBy(() -> sql("ALTER TABLE T2 RESET ('bucket-key')"))
                .rootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Change 'bucket-key' is not supported yet.");

        // merge-engine is immutable
        sql(
                "CREATE TABLE T4 (a STRING, b STRING, c STRING) WITH ('merge-engine' = 'partial-update')");
        sql("INSERT INTO T4 VALUES ('a', 'b', 'c')");
        assertThatThrownBy(() -> sql("ALTER TABLE T4 RESET ('merge-engine')"))
                .rootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Change 'merge-engine' is not supported yet.");

        // sequence.field is immutable
        sql("CREATE TABLE T5 (a STRING, b STRING, c STRING) WITH ('sequence.field' = 'b')");
        sql("INSERT INTO T5 VALUES ('a', 'b', 'c')");
        assertThatThrownBy(() -> sql("ALTER TABLE T5 SET ('sequence.field' = 'c')"))
                .rootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Change 'sequence.field' is not supported yet.");
    }
}
