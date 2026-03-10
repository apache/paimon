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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test case for Lumina vector global index via Flink procedure. */
public class LuminaVectorGlobalIndexITCase extends CatalogITCaseBase {

    private static final String INDEX_TYPE = "lumina-vector-ann";

    @BeforeAll
    static void checkLuminaAvailable() {
        try {
            Class<?> luminaClass = Class.forName("org.aliyun.lumina.Lumina");
            Method isLoaded = luminaClass.getMethod("isLibraryLoaded");
            if (!(boolean) isLoaded.invoke(null)) {
                Method loadLib = luminaClass.getMethod("loadLibrary");
                loadLib.invoke(null);
            }
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Lumina native library not available: " + e.getMessage());
        }
    }

    @Test
    public void testLuminaVectorIndex() throws Catalog.TableNotExistException {
        sql(
                "CREATE TABLE T (id INT, v ARRAY<FLOAT>) WITH ("
                        + "'bucket' = '-1', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'lumina.index.dimension' = '3', "
                        + "'lumina.distance.metric' = 'l2'"
                        + ")");

        String values =
                IntStream.range(0, 100)
                        .mapToObj(
                                i ->
                                        String.format(
                                                "(%d, ARRAY[CAST(%d.0 AS FLOAT), CAST(%d.0 AS FLOAT), CAST(%d.0 AS FLOAT)])",
                                                i, i, i + 1, i + 2))
                        .collect(Collectors.joining(","));
        sql("INSERT INTO T VALUES " + values);

        sql(
                "CALL sys.create_global_index("
                        + "`table` => 'default.T', "
                        + "index_column => 'v', "
                        + "index_type => '"
                        + INDEX_TYPE
                        + "')");

        FileStoreTable table = paimonTable("T");
        List<IndexFileMeta> vectorEntries =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .map(IndexManifestEntry::indexFile)
                        .filter(f -> INDEX_TYPE.equals(f.indexType()))
                        .collect(Collectors.toList());

        assertThat(vectorEntries).isNotEmpty();
        long totalRowCount = vectorEntries.stream().mapToLong(IndexFileMeta::rowCount).sum();
        assertThat(totalRowCount).isEqualTo(100L);
    }
}
