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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test Lookup changelog producer with aggregation tables. */
public class LookupChangelogWithAggITCase extends CatalogITCaseBase {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMultipleCompaction(boolean changelogRowDeduplicate) throws Exception {
        sql(
                "CREATE TABLE T (k INT PRIMARY KEY NOT ENFORCED, v INT) WITH ("
                        + "'bucket'='3', "
                        + "'changelog-producer'='lookup', "
                        + "'changelog-producer.row-deduplicate'='%s', "
                        + "'merge-engine'='aggregation', "
                        + "'fields.v.aggregate-function'='sum')",
                changelogRowDeduplicate);
        BlockingIterator<Row, Row> iterator = streamSqlBlockIter("SELECT * FROM T");

        sql("INSERT INTO T VALUES (1, 1), (2, 2)");
        assertThat(iterator.collect(2)).containsExactlyInAnyOrder(Row.of(1, 1), Row.of(2, 2));

        for (int i = 1; i < 5; i++) {
            sql("INSERT INTO T VALUES (1, 1), (2, 2)");
            assertThat(iterator.collect(4))
                    .containsExactlyInAnyOrder(
                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, i),
                            Row.ofKind(RowKind.UPDATE_BEFORE, 2, 2 * i),
                            Row.ofKind(RowKind.UPDATE_AFTER, 1, i + 1),
                            Row.ofKind(RowKind.UPDATE_AFTER, 2, 2 * (i + 1)));
        }

        for (int i = 5; i < 10; i++) {
            // insert (1,0) to keep the result of sum unchanged.
            sql("INSERT INTO T VALUES (1, 0), (2, 2)");
            if (changelogRowDeduplicate) {
                assertThat(iterator.collect(2))
                        .containsExactlyInAnyOrder(
                                Row.ofKind(RowKind.UPDATE_BEFORE, 2, 2 * i),
                                Row.ofKind(RowKind.UPDATE_AFTER, 2, 2 * (i + 1)));
            } else {
                assertThat(iterator.collect(4))
                        .containsExactlyInAnyOrder(
                                Row.ofKind(RowKind.UPDATE_BEFORE, 1, 5),
                                Row.ofKind(RowKind.UPDATE_BEFORE, 2, 2 * i),
                                Row.ofKind(RowKind.UPDATE_AFTER, 1, 5),
                                Row.ofKind(RowKind.UPDATE_AFTER, 2, 2 * (i + 1)));
            }
        }

        iterator.close();
    }

    @Test
    public void testLookupChangelogProducerWithValueSwitch() throws Exception {
        sql(
                "CREATE TABLE T (k INT PRIMARY KEY NOT ENFORCED, v INT) WITH ("
                        + "'bucket'='3', "
                        + "'changelog-producer'='lookup', "
                        + "'merge-engine'='aggregation', "
                        + "'fields.v.aggregate-function'='sum')");
        BlockingIterator<Row, Row> iterator = streamSqlBlockIter("SELECT * FROM T");

        sql("INSERT INTO T VALUES (1, 1), (2, 2), (1, 3), (1, 4), (1, 5)");
        assertThat(iterator.collect(2)).containsExactlyInAnyOrder(Row.of(1, 13), Row.of(2, 2));

        iterator.close();
    }

    @Test
    public void testLookupChangelogProducerWithProjection() {
        sql(
                "CREATE TABLE T (k INT PRIMARY KEY NOT ENFORCED, v1 INT, v2 INT) WITH ("
                        + "'bucket'='3', "
                        + "'changelog-producer'='lookup', "
                        + "'merge-engine'='aggregation', "
                        + "'fields.v1.aggregate-function'='sum', "
                        + "'fields.v2.aggregate-function'='sum')");

        int times = 3 + ThreadLocalRandom.current().nextInt(3);
        for (int i = 0; i < times; i++) {
            sql("INSERT INTO T VALUES (1, 1, 1), (2, 2, 2)");
        }
        assertThat(sql("SELECT v2 FROM T"))
                .containsExactlyInAnyOrder(Row.of(times), Row.of(times * 2));
    }
}
