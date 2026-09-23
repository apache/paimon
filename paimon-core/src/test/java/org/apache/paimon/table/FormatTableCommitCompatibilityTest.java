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

package org.apache.paimon.table;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.format.FormatTableCommit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;

/** Compatibility tests for the public Format Table commit API. */
class FormatTableCommitCompatibilityTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testLegacyPublicConstructorCanCommitOutsideFormatPackage() throws Exception {
        Path tablePath = new Path(tempDir.toUri());
        try (FormatTableCommit commit =
                new FormatTableCommit(
                        tablePath.toString(),
                        Collections.emptyList(),
                        LocalFileIO.create(),
                        false,
                        "__DEFAULT_PARTITION__",
                        false,
                        Identifier.create("compatibility_db", "compatibility_table"),
                        null,
                        null,
                        null,
                        null,
                        true)) {
            commit.commit(Collections.emptyList());
        }
    }
}
