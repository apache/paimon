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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for system table. */
public class SystemTableITCase extends CatalogTableITCase {

    @Test
    public void testBinlogTableStreamRead() throws Exception {
        sql(
                "CREATE TABLE T (a INT, b INT, primary key (a) NOT ENFORCED) with ('changelog-producer' = 'lookup', "
                        + "'bucket' = '2')");
        BlockingIterator<Row, Row> iterator =
                streamSqlBlockIter("SELECT * FROM T$binlog /*+ OPTIONS('scan.mode' = 'latest') */");
        sql("INSERT INTO T VALUES (1, 2)");
        sql("INSERT INTO T VALUES (1, 3)");
        sql("INSERT INTO T VALUES (2, 2)");
        List<Row> rows = iterator.collect(3);
        assertThat(rows)
                .containsExactly(
                        Row.of("+I", new Integer[] {1}, new Integer[] {2}),
                        Row.of("+U", new Integer[] {1, 1}, new Integer[] {2, 3}),
                        Row.of("+I", new Integer[] {2}, new Integer[] {2}));
        iterator.close();
    }

    @Test
    public void testBinlogTableBatchRead() throws Exception {
        sql(
                "CREATE TABLE T (a INT, b INT, primary key (a) NOT ENFORCED) with ('changelog-producer' = 'lookup', "
                        + "'bucket' = '2')");
        sql("INSERT INTO T VALUES (1, 2)");
        sql("INSERT INTO T VALUES (1, 3)");
        sql("INSERT INTO T VALUES (2, 2)");
        List<Row> rows = sql("SELECT * FROM T$binlog /*+ OPTIONS('scan.mode' = 'latest') */");
        assertThat(rows)
                .containsExactly(
                        Row.of("+I", new Integer[] {1}, new Integer[] {3}),
                        Row.of("+I", new Integer[] {2}, new Integer[] {2}));
    }
}
