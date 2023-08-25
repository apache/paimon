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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.apache.paimon.options.CatalogOptions.DATA_LINEAGE;
import static org.apache.paimon.options.CatalogOptions.TABLE_LINEAGE;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Tests for {@link CatalogOptionsManager}.
 * */
public class CatalogOptionsManagerTest {
    @TempDir static java.nio.file.Path tempDir;
    private static Options withImmutableOptions = new Options();
    private static Options withoutImmutableOptions = new Options();
    private static Options withPartOfImmutableOptions = new Options();

    private static CatalogContext catalogContext;

    private static FileIO fileIO;
    @BeforeAll
    public static void beforeAll() throws IOException {
        withImmutableOptions.set(TABLE_LINEAGE, true);
        withImmutableOptions.set(DATA_LINEAGE, true);
        withImmutableOptions.set(WAREHOUSE, tempDir.toString());

        withoutImmutableOptions.set(WAREHOUSE, tempDir.toString());

        withPartOfImmutableOptions.set(TABLE_LINEAGE, true);
        withPartOfImmutableOptions.set(WAREHOUSE, tempDir.toString());

        catalogContext = CatalogContext.create(withoutImmutableOptions);
        fileIO = FileIO.get(new Path(tempDir.toString()), catalogContext);
    }

    @Test
    public void testSaveImmutableCatalogOptions() throws IOException {
        String warehouse = tempDir.toString();

        CatalogOptionsManager catalogOptionsManager = new CatalogOptionsManager(fileIO, new Path(warehouse));
        CatalogFactory.createCatalog(CatalogContext.create(withImmutableOptions));
        assertThat(fileIO.exists(catalogOptionsManager.getCatalogOptionPath()));
        assertThat(withImmutableOptions.toMap()).containsAllEntriesOf(catalogOptionsManager.immutableOptions());
    }

    @Test
    public void testCatalogOptionsValidate() {
        Options newImmutableCatalogOptions = new Options();
        assertDoesNotThrow(() -> CatalogOptionsManager.validateCatalogOptions(null, newImmutableCatalogOptions));
        newImmutableCatalogOptions.set(TABLE_LINEAGE, true);
        assertDoesNotThrow(() -> CatalogOptionsManager.validateCatalogOptions(null, newImmutableCatalogOptions));
        newImmutableCatalogOptions.set(DATA_LINEAGE, true);
        assertDoesNotThrow(() -> CatalogOptionsManager.validateCatalogOptions(null, newImmutableCatalogOptions));
        newImmutableCatalogOptions.set(TABLE_LINEAGE, false);
        assertThatThrownBy(() -> CatalogOptionsManager.validateCatalogOptions(null, newImmutableCatalogOptions))
                .isInstanceOf(UnsupportedOperationException.class);
        newImmutableCatalogOptions.set(TABLE_LINEAGE, true);

        Options originImmutableCatalogOptions = new Options();
        assertThatThrownBy(() -> CatalogOptionsManager.validateCatalogOptions(originImmutableCatalogOptions, newImmutableCatalogOptions))
                .isInstanceOf(IllegalStateException.class);
        originImmutableCatalogOptions.set(TABLE_LINEAGE, true);
        assertThatThrownBy(() -> CatalogOptionsManager.validateCatalogOptions(originImmutableCatalogOptions, newImmutableCatalogOptions))
                .isInstanceOf(IllegalStateException.class);
        originImmutableCatalogOptions.set(DATA_LINEAGE, true);
        assertDoesNotThrow(() -> CatalogOptionsManager.validateCatalogOptions(originImmutableCatalogOptions, newImmutableCatalogOptions));
    }

    @Test
    public void testCreatingCatalogWithConflictOptions() throws IOException {
        // session1: without immutable options, session2: with immutable options, throw IllegalStateException
        CatalogFactory.createCatalog(CatalogContext.create(withoutImmutableOptions));
        assertThatThrownBy(() -> CatalogFactory.createCatalog(CatalogContext.create(withImmutableOptions)))
                .isInstanceOf(IllegalStateException.class);
        cleanCatalogDir();

        // session1: without immutable options, session2: without immutable options, succeeded
        CatalogFactory.createCatalog(CatalogContext.create(withoutImmutableOptions));
        assertDoesNotThrow(() -> CatalogFactory.createCatalog(CatalogContext.create(withoutImmutableOptions)));
        cleanCatalogDir();

        // session1: with immutable options, session2: with the same immutable options, succeeded
        CatalogFactory.createCatalog(CatalogContext.create(withImmutableOptions));
        assertDoesNotThrow(() -> CatalogFactory.createCatalog(CatalogContext.create(withImmutableOptions)));
        cleanCatalogDir();

        // session1: with immutable options, session2: with different immutable options, throw IllegalStateException
        CatalogFactory.createCatalog(CatalogContext.create(withImmutableOptions));
        assertThatThrownBy(() -> CatalogFactory.createCatalog(CatalogContext.create(withPartOfImmutableOptions)))
                .isInstanceOf(IllegalStateException.class);
        cleanCatalogDir();

        // session1: with immutable options, session2: without immutable options, throw IllegalStateException
        CatalogFactory.createCatalog(CatalogContext.create(withImmutableOptions));
        assertThatThrownBy(() -> CatalogFactory.createCatalog(CatalogContext.create(withoutImmutableOptions)))
                .isInstanceOf(IllegalStateException.class);
        cleanCatalogDir();
    }

    private static void cleanCatalogDir() throws IOException {
        fileIO.delete(new Path(tempDir.toString()), true);
    }
}
