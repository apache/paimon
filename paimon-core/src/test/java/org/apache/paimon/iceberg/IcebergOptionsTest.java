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

package org.apache.paimon.iceberg;

import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link IcebergOptions#metastoreDatabases}. */
public class IcebergOptionsTest {

    private static final String FALLBACK_DB = "default_db";

    @Test
    public void testSingleDatabase() {
        Options options = new Options();
        options.set(IcebergOptions.METASTORE_DATABASE, "mydb");

        List<String> databases = IcebergOptions.metastoreDatabases(options, FALLBACK_DB);

        assertThat(databases).containsExactly("mydb");
    }

    @Test
    public void testMultipleDatabases() {
        Options options = new Options();
        options.set(IcebergOptions.METASTORE_DATABASE, "db1;db2;db3");

        List<String> databases = IcebergOptions.metastoreDatabases(options, FALLBACK_DB);

        assertThat(databases).containsExactly("db1", "db2", "db3");
    }

    @Test
    public void testNoDatabaseConfigFallsBack() {
        Options options = new Options();

        List<String> databases = IcebergOptions.metastoreDatabases(options, FALLBACK_DB);

        assertThat(databases).containsExactly("default_db");
    }

    @Test
    public void testEmptyDatabaseStringFallsBack() {
        Options options = new Options();
        options.set(IcebergOptions.METASTORE_DATABASE, "");

        List<String> databases = IcebergOptions.metastoreDatabases(options, FALLBACK_DB);

        assertThat(databases).containsExactly("default_db");
    }

    @Test
    public void testWhitespaceAroundSemicolon() {
        Options options = new Options();
        options.set(IcebergOptions.METASTORE_DATABASE, " db1 ; db2 ; db3 ");

        List<String> databases = IcebergOptions.metastoreDatabases(options, FALLBACK_DB);

        assertThat(databases).containsExactly("db1", "db2", "db3");
    }
}
