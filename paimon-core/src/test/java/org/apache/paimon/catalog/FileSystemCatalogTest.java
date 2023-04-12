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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileSystemCatalog}. */
public class FileSystemCatalogTest extends CatalogTestBase {

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        catalog = new FileSystemCatalog(fileIO, new Path(warehouse));
    }

    @Test
    @Override
    public void testListDatabasesWhenNoDatabases() {
        // List databases returns an empty list when there are no databases
        List<String> databases = catalog.listDatabases();
        assertThat(databases).isEmpty();
    }
}
