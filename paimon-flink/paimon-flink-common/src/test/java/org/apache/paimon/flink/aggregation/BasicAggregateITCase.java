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

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Test for aggregation merge engine. */
public class BasicAggregateITCase extends CatalogITCaseBase {

    @Override
    protected List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE T ("
                        + "k INT,"
                        + "v INT,"
                        + "d INT,"
                        + "PRIMARY KEY (k, d) NOT ENFORCED) PARTITIONED BY (d) "
                        + " WITH ('merge-engine'='aggregation', "
                        + "'fields.v.aggregate-function'='sum',"
                        + "'local-merge-buffer-size'='5m'"
                        + ");");
    }

    @Test
    public void testLocalMerge() {
        sql("INSERT INTO T VALUES(1, 1, 1), (2, 1, 1), (1, 2, 1)");
        assertThat(batchSql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(1, 3, 1), Row.of(2, 1, 1));
    }

    @Test
    public void testMergeRead() {
        sql("INSERT INTO T VALUES(1, 1, 1), (2, 1, 1)");
        sql("INSERT INTO T VALUES(1, 2, 1)");
        assertThat(batchSql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(1, 3, 1), Row.of(2, 1, 1));
        // filter
        assertThat(batchSql("SELECT * FROM T where v = 3"))
                .containsExactlyInAnyOrder(Row.of(1, 3, 1));
        assertThat(batchSql("SELECT * FROM T where v = 1"))
                .containsExactlyInAnyOrder(Row.of(2, 1, 1));
    }

    @Test
    public void testSequenceFieldWithDefaultAgg() {
        sql(
                "CREATE TABLE seq_default_agg ("
                        + " pk INT PRIMARY KEY NOT ENFORCED,"
                        + " seq INT,"
                        + " v INT) WITH ("
                        + " 'merge-engine'='aggregation',"
                        + " 'sequence.field'='seq',"
                        + " 'fields.default-aggregate-function'='sum'"
                        + ")");

        sql("INSERT INTO seq_default_agg VALUES (0, 1, 1)");
        sql("INSERT INTO seq_default_agg VALUES (0, 2, 2)");

        assertThat(sql("SELECT * FROM seq_default_agg")).containsExactly(Row.of(0, 2, 3));
    }
}
