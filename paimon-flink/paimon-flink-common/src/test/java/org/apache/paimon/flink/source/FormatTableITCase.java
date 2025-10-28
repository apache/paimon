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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.flink.RESTCatalogITCaseBase;
import org.apache.paimon.rest.RESTToken;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for format table. */
public class FormatTableITCase extends RESTCatalogITCaseBase {

    @Test
    public void testParquetFileFormat() {
        String bigDecimalStr = "10.001";
        Decimal decimal = Decimal.fromBigDecimal(new BigDecimal(bigDecimalStr), 8, 3);
        String tableName = "format_table_test";
        Identifier identifier = Identifier.create("default", tableName);
        sql(
                "CREATE TABLE %s (a DECIMAL(8, 3), b INT, c INT) WITH ('file.format'='parquet', 'type'='format-table')",
                tableName);
        RESTToken expiredDataToken =
                new RESTToken(
                        ImmutableMap.of(
                                "akId", "akId-expire", "akSecret", UUID.randomUUID().toString()),
                        System.currentTimeMillis() + 1000_000);
        restCatalogServer.setDataToken(identifier, expiredDataToken);
        sql("INSERT INTO %s VALUES (%s, 1, 1), (%s, 2, 2)", tableName, decimal, decimal);
        assertThat(sql("SELECT a, b FROM %s", tableName))
                .containsExactlyInAnyOrder(
                        Row.of(new BigDecimal(bigDecimalStr), 1),
                        Row.of(new BigDecimal(bigDecimalStr), 2));
        sql("Drop TABLE %s", tableName);
    }
}
