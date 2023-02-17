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

package org.apache.flink.table.store.file.catalog;

import org.apache.flink.table.store.catalog.CatalogContext;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.fs.local.LocalFileIO;
import org.apache.flink.table.store.options.Options;
import org.apache.flink.table.store.table.TableType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.apache.flink.table.store.options.CatalogOptions.TABLE_TYPE;
import static org.apache.flink.table.store.options.CatalogOptions.WAREHOUSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CatalogFactory}. */
public class CatalogFactoryTest {

    @Test
    public void testAutomaticCreatePath(@TempDir java.nio.file.Path path) {
        Path root = new Path(path.toUri().toString());
        Options options = new Options();
        options.set(WAREHOUSE, new Path(root, "warehouse").toString());
        assertThat(CatalogFactory.createCatalog(CatalogContext.create(options)).listDatabases())
                .isEmpty();
    }

    @Test
    public void testNotDirectory(@TempDir java.nio.file.Path path) throws IOException {
        Path root = new Path(path.toUri().toString());
        Path warehouse = new Path(root, "warehouse");
        LocalFileIO.create().writeFileUtf8(warehouse, "");
        Options options = new Options();
        options.set(WAREHOUSE, warehouse.toString());
        assertThatThrownBy(() -> CatalogFactory.createCatalog(CatalogContext.create(options)))
                .hasMessageContaining("should be a directory");
    }

    @Test
    public void testNonManagedTable(@TempDir java.nio.file.Path path) {
        Path root = new Path(path.toUri().toString());
        Options options = new Options();
        options.set(WAREHOUSE, new Path(root, "warehouse").toString());
        options.set(TABLE_TYPE, TableType.EXTERNAL);
        assertThatThrownBy(() -> CatalogFactory.createCatalog(CatalogContext.create(options)))
                .hasMessageContaining("Only managed table is supported in File system catalog.");
    }
}
