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

package org.apache.paimon.tantivy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/** Smoke test for Tantivy JNI. */
class TantivyJniTest {

    @Test
    void testWriteAndSearch(@TempDir Path tempDir) {
        String indexPath = tempDir.resolve("test_index").toString();

        try (TantivyIndexWriter writer = new TantivyIndexWriter(indexPath)) {
            writer.addDocument(1L, "Apache Paimon is a streaming data lake platform");
            writer.addDocument(2L, "Tantivy is a full-text search engine written in Rust");
            writer.addDocument(3L, "Paimon supports real-time data ingestion");
            writer.commit();
        }

        try (TantivySearcher searcher = new TantivySearcher(indexPath)) {
            SearchResult result = searcher.search("paimon", 10);
            assertTrue(result.size() > 0, "Should find at least one result");
            assertEquals(result.getRowIds().length, result.getScores().length);

            // Both doc 1 and doc 3 mention "paimon"
            assertEquals(2, result.size());
            for (int i = 0; i < result.size(); i++) {
                long rowId = result.getRowIds()[i];
                assertTrue(rowId == 1L || rowId == 3L, "Unexpected rowId: " + rowId);
                assertTrue(result.getScores()[i] > 0, "Score should be positive");
            }

            // Scores should be descending
            if (result.size() > 1) {
                assertTrue(result.getScores()[0] >= result.getScores()[1]);
            }
        }
    }
}
