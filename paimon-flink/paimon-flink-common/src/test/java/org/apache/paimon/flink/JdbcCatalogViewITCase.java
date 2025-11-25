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

import org.apache.paimon.jdbc.JdbcCatalog;
import org.apache.paimon.options.CatalogOptions;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for JDBC Catalog View support. */
public class JdbcCatalogViewITCase extends CatalogITCaseBase {

    private static final String DATABASE_NAME = "test_db";
    private static final String TABLE_NAME = "test_table";

    @TempDir java.nio.file.Path tempFile;

    @BeforeEach
    @Override
    public void before() throws IOException {
        super.before();
        sql(String.format("CREATE DATABASE %s", DATABASE_NAME));
        sql(String.format("USE %s", DATABASE_NAME));
        sql(
                String.format(
                        "CREATE TABLE %s.%s (id INT, name STRING, amount DOUBLE)",
                        DATABASE_NAME, TABLE_NAME));
        sql(
                String.format(
                        "INSERT INTO %s.%s VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 150.0)",
                        DATABASE_NAME, TABLE_NAME));
    }

    @Override
    protected Map<String, String> catalogOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("metastore", "jdbc");
        options.put(
                CatalogOptions.URI.key(),
                "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", ""));
        options.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
        options.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
        // Disable lock for simpler testing
        options.put(CatalogOptions.LOCK_ENABLED.key(), "false");
        return options;
    }

    @Override
    protected String getTempDirPath() {
        return tempFile.toUri().toString();
    }

    @Test
    public void testCreateAndQueryView() {
        // Create a view
        String viewName = "sales_view";
        sql(
                String.format(
                        "CREATE VIEW %s.%s AS SELECT name, amount FROM %s.%s WHERE amount > 100",
                        DATABASE_NAME, viewName, DATABASE_NAME, TABLE_NAME));

        // Query the view
        List<Row> result = sql(String.format("SELECT * FROM %s.%s", DATABASE_NAME, viewName));
        assertThat(result).hasSize(2);
        assertThat(result.toString()).contains("Bob");
        assertThat(result.toString()).contains("Charlie");
    }

    @Test
    public void testShowViews() {
        // Create multiple views
        sql(
                String.format(
                        "CREATE VIEW %s.view1 AS SELECT * FROM %s.%s",
                        DATABASE_NAME, DATABASE_NAME, TABLE_NAME));
        sql(
                String.format(
                        "CREATE VIEW %s.view2 AS SELECT id, name FROM %s.%s",
                        DATABASE_NAME, DATABASE_NAME, TABLE_NAME));

        // List views
        List<Row> result = sql("SHOW VIEWS");
        assertThat(result).hasSize(2);
        assertThat(result.toString()).contains("view1");
        assertThat(result.toString()).contains("view2");
    }

    @Test
    public void testDropView() {
        // Create a view
        String viewName = "temp_view";
        sql(
                String.format(
                        "CREATE VIEW %s.%s AS SELECT * FROM %s.%s",
                        DATABASE_NAME, viewName, DATABASE_NAME, TABLE_NAME));

        // Verify view exists
        List<Row> views = sql("SHOW VIEWS");
        assertThat(views.toString()).contains(viewName);

        // Drop the view
        sql(String.format("DROP VIEW %s.%s", DATABASE_NAME, viewName));

        // Verify view is dropped
        views = sql("SHOW VIEWS");
        assertThat(views.toString()).doesNotContain(viewName);
    }

    @Test
    public void testViewWithAggregation() {
        // Create a view with aggregation
        String viewName = "agg_view";
        sql(
                String.format(
                        "CREATE VIEW %s.%s AS SELECT COUNT(*) as cnt, SUM(amount) as total FROM %s.%s",
                        DATABASE_NAME, viewName, DATABASE_NAME, TABLE_NAME));

        // Query the view
        List<Row> result = sql(String.format("SELECT * FROM %s.%s", DATABASE_NAME, viewName));
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getField(0)).isEqualTo(3L); // count
        assertThat((Double) result.get(0).getField(1)).isEqualTo(450.0); // sum
    }

    @Test
    public void testCreateViewIfNotExists() {
        String viewName = "idempotent_view";

        // Create view first time
        sql(
                String.format(
                        "CREATE VIEW %s.%s AS SELECT * FROM %s.%s",
                        DATABASE_NAME, viewName, DATABASE_NAME, TABLE_NAME));

        // Create view again with IF NOT EXISTS - should not throw error
        sql(
                String.format(
                        "CREATE VIEW IF NOT EXISTS %s.%s AS SELECT id FROM %s.%s",
                        DATABASE_NAME, viewName, DATABASE_NAME, TABLE_NAME));

        // Original view should remain unchanged
        List<Row> result =
                sql(String.format("SELECT * FROM %s.%s LIMIT 1", DATABASE_NAME, viewName));
        assertThat(result.get(0).getArity()).isEqualTo(3); // original view has 3 columns
    }

    @Test
    public void testDropViewIfExists() {
        String viewName = "maybe_exists_view";

        // Drop non-existent view with IF EXISTS - should not throw error
        sql(String.format("DROP VIEW IF EXISTS %s.%s", DATABASE_NAME, viewName));

        // Create and drop
        sql(
                String.format(
                        "CREATE VIEW %s.%s AS SELECT * FROM %s.%s",
                        DATABASE_NAME, viewName, DATABASE_NAME, TABLE_NAME));
        sql(String.format("DROP VIEW IF EXISTS %s.%s", DATABASE_NAME, viewName));

        // Verify it's gone
        List<Row> views = sql("SHOW VIEWS");
        assertThat(views.toString()).doesNotContain(viewName);
    }

    @Test
    public void testShowCreateView() {
        String viewName = "describable_view";
        String query =
                String.format(
                        "SELECT `name`, `amount` FROM `%s`.`%s` WHERE `amount` > 100",
                        DATABASE_NAME, TABLE_NAME);
        sql(String.format("CREATE VIEW %s.%s AS %s", DATABASE_NAME, viewName, query));

        // Show create view
        List<Row> result = sql(String.format("SHOW CREATE VIEW %s.%s", DATABASE_NAME, viewName));
        assertThat(result).hasSize(1);
        String createStatement = result.get(0).toString();
        assertThat(createStatement).contains(viewName);
        assertThat(createStatement).contains("amount");
    }
}
