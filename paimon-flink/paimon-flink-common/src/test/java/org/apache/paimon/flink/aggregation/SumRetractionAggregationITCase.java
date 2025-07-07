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

package org.apache.paimon.flink.aggregation;

import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for sum aggregate function retraction. */
public class SumRetractionAggregationITCase extends CatalogITCaseBase {
    @Override
    protected List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE T ("
                        + "k INT,"
                        + "b Decimal(12, 2),"
                        + "PRIMARY KEY (k) NOT ENFORCED)"
                        + " WITH ('merge-engine'='aggregation', "
                        + "'changelog-producer' = 'full-compaction',"
                        + "'fields.b.aggregate-function'='sum'"
                        + ");");
    }

    @Test
    public void testRetraction() throws Exception {
        sql("CREATE TABLE INPUT (" + "k INT," + "b INT," + "PRIMARY KEY (k) NOT ENFORCED);");
        CloseableIterator<Row> insert =
                streamSqlIter("INSERT INTO T SELECT k, SUM(b) FROM INPUT GROUP BY k;");
        BlockingIterator<Row, Row> select = streamSqlBlockIter("SELECT * FROM T");

        sql("INSERT INTO INPUT VALUES (1, 1), (2, 2)");
        assertThat(select.collect(2))
                .containsExactlyInAnyOrder(
                        Row.of(1, BigDecimal.valueOf(100, 2)),
                        Row.of(2, BigDecimal.valueOf(200, 2)));

        sql("INSERT INTO INPUT VALUES (1, 3), (2, 4)");
        assertThat(select.collect(4))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, BigDecimal.valueOf(100, 2)),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, BigDecimal.valueOf(300, 2)),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 2, BigDecimal.valueOf(200, 2)),
                        Row.ofKind(RowKind.UPDATE_AFTER, 2, BigDecimal.valueOf(400, 2)));

        select.close();
        insert.close();
    }
}
