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

package org.apache.paimon.flink.source.shardread;

import org.apache.paimon.flink.CatalogITCaseBase;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

/** IT case for shard read. */
public class ShardReadITCase extends CatalogITCaseBase {

    @Override
    public List<String> ddl() {
        return ImmutableList.of("CREATE TABLE T (" + "a INT, b INT, c STRING) PARTITIONED BY (a);");
    }

    @BeforeEach
    @Override
    public void before() throws IOException {
        super.before();
    }

    @Test
    public void testAllTaskReadRealSplit() {
        batchSql(
                "INSERT INTO T /*+ OPTIONS('target-file-size'='1b') */ VALUES (1, 1, '1'), (1, 2, '2'), (2, 3, '3'), (3, 3, '3');");
        String sql = "SELECT * FROM T /*+ OPTIONS('scan.split-enumerator.mode'='shard_read') */";
        List<Row> result = batchSql(sql);
        Assertions.assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, 1, 1, "1"),
                        Row.ofKind(RowKind.INSERT, 1, 2, "2"),
                        Row.ofKind(RowKind.INSERT, 2, 3, "3"),
                        Row.ofKind(RowKind.INSERT, 3, 3, "3"));
    }

    @Test
    public void testSomeTaskNotReadRealSplit() {
        batchSql("INSERT INTO T VALUES (1, 1, '1'), (1, 2, '2'), (2, 3, '3'), (3, 3, '3')");
        String sql =
                "SELECT * FROM T /*+ OPTIONS('scan.split-enumerator.mode'='shard_read') */ where a = 1 limit 1";
        List<Row> result = batchSql(sql);
        Assertions.assertThat(result)
                .containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, 1, 1, "1"));
    }
}
