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

package org.apache.paimon.catalog;

import org.apache.paimon.fs.Path;
import org.apache.paimon.jdbc.JdbcCatalog;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileSystemCatalog}. */
public class FileSystemCatalogTest extends CatalogTestBase {

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        catalog = initCatalog(Maps.newHashMap());
    }

    private FileSystemCatalog initCatalog(Map<String, String> props) {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(
                AbstractCatalog.LOCK_PROP_PREFIX + CatalogOptions.URI.key(),
                "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", ""));

        properties.put(
                AbstractCatalog.LOCK_PROP_PREFIX + JdbcCatalog.PROPERTY_PREFIX + "username",
                "user");
        properties.put(
                AbstractCatalog.LOCK_PROP_PREFIX + JdbcCatalog.PROPERTY_PREFIX + "password",
                "password");
        properties.put(CatalogOptions.WAREHOUSE.key(), warehouse);
        properties.put(CatalogOptions.LOCK_ENABLED.key(), "true");
        properties.put(CatalogOptions.LOCK_TYPE.key(), "jdbc");
        properties.putAll(props);
        FileSystemCatalog catalog =
                new FileSystemCatalog(fileIO, new Path(warehouse), Options.fromMap(properties));
        return catalog;
    }

    @Override
    public void testListDatabasesWhenNoDatabases() {
        List<String> databases = catalog.listDatabases();
        assertThat(databases).isEqualTo(new ArrayList<>());
    }
}
