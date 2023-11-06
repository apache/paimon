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

package org.apache.paimon.lineage;

import org.apache.paimon.options.Options;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.lineage.LineageMetaUtils.SINK_TABLE_LINEAGE;
import static org.apache.paimon.lineage.LineageMetaUtils.SOURCE_TABLE_LINEAGE;
import static org.apache.paimon.options.CatalogOptions.JDBC_AUTO_DDL;
import static org.apache.paimon.options.CatalogOptions.JDBC_URL;
import static org.apache.paimon.options.CatalogOptions.LINEAGE_META;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for jdbc lineage meta. */
class JdbcLineageMetaTest {
    private static final String DB_NAME = "lineage_meta";
    private static final String URL = "jdbc:derby:memory:" + DB_NAME + ";create=true";

    @BeforeAll
    static void setUp() throws Exception {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
    }

    @Test
    void testLineageMetaDDL() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(LINEAGE_META.key(), "jdbc");
        config.put(JDBC_URL.key(), URL);
        config.put(JDBC_AUTO_DDL.key(), "true");

        JdbcLineageMetaFactory factory = new JdbcLineageMetaFactory();
        try (JdbcLineageMeta lineageMeta =
                (JdbcLineageMeta) factory.create(() -> Options.fromMap(config))) {
            Statement statement = lineageMeta.statement();
            assertThatThrownBy(
                            () ->
                                    statement.executeQuery(
                                            String.format("SELECT * FROM %s", "not_exist_table")))
                    .hasMessage("Table/View 'NOT_EXIST_TABLE' does not exist.");

            // Validate source and sink tables are existing.
            try (ResultSet resultSet =
                    statement.executeQuery(
                            String.format("SELECT * FROM %s", SOURCE_TABLE_LINEAGE))) {
                assertThat(resultSet.next()).isFalse();
            }
            try (ResultSet resultSet =
                    statement.executeQuery(String.format("SELECT * FROM %s", SINK_TABLE_LINEAGE))) {
                assertThat(resultSet.next()).isFalse();
            }
        }
    }
}
