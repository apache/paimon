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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/** ITCase for deletion vector table. */
public class DeletionVectorITCase extends CatalogITCaseBase {

    @ParameterizedTest
    @ValueSource(strings = {"none", "lookup"})
    public void testStreamingReadDVTable(String changelogProducer) throws Exception {
        sql(
                String.format(
                        "CREATE TABLE T (id INT PRIMARY KEY NOT ENFORCED, name STRING) "
                                + "WITH ('deletion-vectors.enabled' = 'true', 'changelog-producer' = '%s')",
                        changelogProducer));

        sql("INSERT INTO T VALUES (1, '111111111'), (2, '2'), (3, '3'), (4, '4')");

        sql("INSERT INTO T VALUES (2, '2_1'), (3, '3_1')");

        sql("INSERT INTO T VALUES (2, '2_2'), (4, '4_1')");

        // test read from APPEND snapshot
        try (BlockingIterator<Row, Row> iter =
                streamSqlBlockIter(
                        "SELECT * FROM T /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id' = '3') */"); ) {
            assertThat(iter.collect(12))
                    .containsExactlyInAnyOrder(
                            Row.ofKind(RowKind.INSERT, 1, "111111111"),
                            Row.ofKind(RowKind.INSERT, 2, "2"),
                            Row.ofKind(RowKind.INSERT, 3, "3"),
                            Row.ofKind(RowKind.INSERT, 4, "4"),
                            Row.ofKind(RowKind.UPDATE_BEFORE, 2, "2"),
                            Row.ofKind(RowKind.UPDATE_AFTER, 2, "2_1"),
                            Row.ofKind(RowKind.UPDATE_BEFORE, 3, "3"),
                            Row.ofKind(RowKind.UPDATE_AFTER, 3, "3_1"),
                            Row.ofKind(RowKind.UPDATE_BEFORE, 2, "2_1"),
                            Row.ofKind(RowKind.UPDATE_AFTER, 2, "2_2"),
                            Row.ofKind(RowKind.UPDATE_BEFORE, 4, "4"),
                            Row.ofKind(RowKind.UPDATE_AFTER, 4, "4_1"));
        }

        // test read from COMPACT snapshot
        try (BlockingIterator<Row, Row> iter =
                streamSqlBlockIter(
                        "SELECT * FROM T /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id' = '4') */"); ) {
            assertThat(iter.collect(8))
                    .containsExactlyInAnyOrder(
                            Row.ofKind(RowKind.INSERT, 1, "111111111"),
                            Row.ofKind(RowKind.INSERT, 2, "2_1"),
                            Row.ofKind(RowKind.INSERT, 3, "3_1"),
                            Row.ofKind(RowKind.INSERT, 4, "4"),
                            Row.ofKind(RowKind.UPDATE_BEFORE, 2, "2_1"),
                            Row.ofKind(RowKind.UPDATE_AFTER, 2, "2_2"),
                            Row.ofKind(RowKind.UPDATE_BEFORE, 4, "4"),
                            Row.ofKind(RowKind.UPDATE_AFTER, 4, "4_1"));
        }
    }
}
