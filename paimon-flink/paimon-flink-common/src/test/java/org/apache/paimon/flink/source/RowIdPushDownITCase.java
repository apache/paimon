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

package org.apache.paimon.flink.source;

import org.apache.paimon.flink.CatalogITCaseBase;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.types.Row;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** ITCase for RowId push down. */
public class RowIdPushDownITCase extends CatalogITCaseBase {

    @Override
    public List<String> ddl() {
        return ImmutableList.of(
                "CREATE TABLE T ("
                        + "a INT, b INT, c STRING) PARTITIONED BY (a) "
                        + "WITH ('row-tracking.enabled'='true');");
    }

    @BeforeEach
    @Override
    public void before() throws IOException {
        super.before();
        setParallelism(1);
        batchSql("INSERT INTO T VALUES (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4')");
    }

    @Test
    public void testSimplePredicate() throws Exception {
        List<Row> result;

        // 1. in
        result = sql("SELECT * FROM T$row_tracking WHERE _ROW_ID IN (1, 2)");
        assertFilteredResultEquals(result, id -> id == 1 || id == 2);

        // 2. equal
        result = sql("SELECT * FROM T$row_tracking WHERE _ROW_ID = 3");
        assertFilteredResultEquals(result, id -> id == 3);

        // 3. empty
        result = sql("SELECT * FROM T$row_tracking WHERE _ROW_ID IN (5, 6)");
        Assertions.assertThat(result).isEmpty();
    }

    @Test
    public void testCompoundPredicate() throws Exception {
        List<Row> result;

        // 1. AND
        result = sql("SELECT * FROM T$row_tracking WHERE _ROW_ID IN (1, 2, 3) AND _ROW_ID = 3");
        assertFilteredResultEquals(result, id -> id == 3);

        // 2. OR
        result = sql("SELECT * FROM T$row_tracking WHERE _ROW_ID IN (1, 2) OR _ROW_ID = 3");
        assertFilteredResultEquals(result, id -> id == 1 || id == 2 || id == 3);

        // 3. AND with empty result
        result = sql("SELECT * FROM T$row_tracking WHERE _ROW_ID IN (1, 2) AND _ROW_ID = 3");
        Assertions.assertThat(result).isEmpty();
    }

    private void assertFilteredResultEquals(List<Row> result, Predicate<Long> rowIdFilter) {
        List<Row> fullScan = sql("SELECT * FROM T$row_tracking");
        Assertions.assertThat(result)
                .containsExactlyInAnyOrderElementsOf(
                        fullScan.stream()
                                .filter(
                                        row -> {
                                            Long rowId = row.getFieldAs(3);
                                            return rowIdFilter.test(rowId);
                                        })
                                .collect(Collectors.toList()));
    }
}
