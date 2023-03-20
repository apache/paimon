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

import static org.assertj.core.api.Assertions.assertThat;

/** Test Lookup changelog producer with aggregation tables. */
public class LookupChangelogWithAggITCase extends CatalogITCaseBase {

    @Test
    public void testMultipleCompaction() throws Exception {
        sql(
                "CREATE TABLE T (k INT PRIMARY KEY NOT ENFORCED, v INT) WITH ("
                        + "'bucket'='3', "
                        + "'changelog-producer'='lookup', "
                        + "'merge-engine'='aggregation', "
                        + "'fields.v.aggregate-function'='sum')");
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

        iterator.close();
    }
}
