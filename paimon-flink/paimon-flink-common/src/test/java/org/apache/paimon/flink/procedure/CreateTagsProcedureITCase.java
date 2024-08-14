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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.utils.SnapshotNotExistException;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;

/** IT Case for {@link CreateTagProcedure}. */
public class CreateTagsProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testCreateTags() {
        sql(
                "CREATE TABLE T ("
                        + " k STRING,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt) NOT ENFORCED"
                        + ") PARTITIONED BY (dt) WITH ("
                        + " 'bucket' = '1'"
                        + ")");
        sql("insert into T values('k', '2024-01-01')");
        sql("insert into T values('k2', '2024-01-02')");

        sql("CALL sys.create_tag('default.T', 'tag1')");

        assertThat(
                        sql("select * from T /*+ OPTIONS('scan.tag-name'='tag1') */").stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder("+I[k2, 2024-01-02]", "+I[k, 2024-01-01]");

        sql("CALL sys.create_tag('default.T', 'tag2', 1)");

        assertThat(
                        sql("select * from T /*+ OPTIONS('scan.tag-name'='tag2') */").stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder("+I[k, 2024-01-01]");
    }

    @Test
    public void testThrowSnapshotNotExistException() {
        sql(
                "CREATE TABLE T ("
                        + " k STRING,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt) NOT ENFORCED"
                        + ") PARTITIONED BY (dt) WITH ("
                        + " 'bucket' = '1'"
                        + ")");

        assertThatException()
                .isThrownBy(() -> sql("CALL sys.create_tag('default.T', 'tag1')"))
                .withRootCauseInstanceOf(SnapshotNotExistException.class)
                .withMessageContaining("Cannot create tag because latest snapshot doesn't exist.");

        assertThatException()
                .isThrownBy(() -> sql("CALL sys.create_tag('default.T', 'tag1', 1)"))
                .withRootCauseInstanceOf(SnapshotNotExistException.class)
                .withMessageContaining(
                        "Cannot create tag because given snapshot #1 doesn't exist.");
    }
}
