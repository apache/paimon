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

package org.apache.paimon.tests;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/** E2E test for vector-store with data evolution. */
public class FlinkVectorStoreE2eTest extends E2eTestBase {

    @Test
    public void testVectorStoreTable() throws Exception {
        Random rnd = new Random(System.currentTimeMillis());
        int vectorDim = rnd.nextInt(10) + 1;
        final int itemNum = rnd.nextInt(3) + 1;

        String catalogDdl =
                String.format(
                        "CREATE CATALOG ts_catalog WITH (\n"
                                + "    'type' = 'paimon',\n"
                                + "    'warehouse' = '%s'\n"
                                + ");",
                        TEST_DATA_DIR + "/" + UUID.randomUUID() + ".store");

        String useCatalogCmd = "USE CATALOG ts_catalog;";

        String createTableDdl =
                String.format(
                        "CREATE TABLE IF NOT EXISTS ts_table (\n"
                                + "    id BIGINT,\n"
                                + "    embed ARRAY<FLOAT>\n"
                                + ") WITH (\n"
                                + "    'file.format' = 'parquet',\n"
                                + "    'file.compression' = 'none',\n"
                                + "    'row-tracking.enabled' = 'true',\n"
                                + "    'data-evolution.enabled' = 'true',\n"
                                + "    'vector-store.format' = 'json',\n"
                                + "    'vector-store.fields' = 'embed',\n"
                                + "    'field.embed.vector-dim' = '%d'\n"
                                + ");",
                        vectorDim);

        float[][] vectors = new float[itemNum][vectorDim];
        byte[] vectorDataBuf = new byte[vectorDim];
        for (int i = 0; i < itemNum; ++i) {
            vectors[i] = new float[vectorDim];
            rnd.nextBytes(vectorDataBuf);
            for (int j = 0; j < vectorDim; ++j) {
                vectors[i][j] = vectorDataBuf[j];
            }
        }

        List<String> values = new ArrayList<>();
        String[] expected = new String[itemNum];
        for (int id = 0; id < itemNum; ++id) {
            values.add(String.format("(%d, %s)", id, arrayLiteral(vectors[id])));
            expected[id] = String.format("%d, %s", id, Arrays.toString(vectors[id]));
        }

        runBatchSql(
                "INSERT INTO ts_table VALUES " + String.join(", ", values) + ";",
                catalogDdl,
                useCatalogCmd,
                createTableDdl);

        runBatchSql(
                "INSERT INTO result1 SELECT * FROM ts_table;",
                catalogDdl,
                useCatalogCmd,
                createTableDdl,
                createResultSink("result1", "id BIGINT, embed ARRAY<FLOAT>"));
        checkResult(expected);
        clearCurrentResults();

        runBatchSql(
                "INSERT INTO result2 SELECT "
                        + "COUNT(*) AS total, "
                        + "SUM(CASE WHEN file_path LIKE '%.vector-store%.json' THEN 1 ELSE 0 END) "
                        + "AS vector_files "
                        + "FROM \\`ts_table\\$files\\`;",
                catalogDdl,
                useCatalogCmd,
                createTableDdl,
                createResultSink("result2", "total BIGINT, vector_files BIGINT"));
        checkResult("2, 1");
    }

    private String arrayLiteral(float[] vector) {
        return "ARRAY" + Arrays.toString(vector);
    }
}
