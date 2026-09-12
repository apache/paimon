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

package org.apache.paimon.lumina.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests that {@link LuminaVectorGlobalIndexWriter.FileBackedDataset} releases the file it opened
 * when the rest of its construction fails.
 */
public class LuminaFileBackedDatasetCloseTest {

    @TempDir Path tempDir;

    /**
     * checkedRecordSize rejects a dimension whose record does not fit the read buffer, and it runs
     * after the file is open. A caller's try-with-resources never sees an object whose constructor
     * threw, so nothing would close that handle.
     */
    @Test
    public void testFailedConstructionReleasesTheFile() throws Exception {
        File fdDir = new File("/dev/fd");
        assumeTrue(fdDir.isDirectory() && fdDir.list() != null, "needs /dev/fd");

        File file = new File(tempDir.toFile(), "vectors.bin");
        try (FileOutputStream out = new FileOutputStream(file)) {
            out.write(new byte[64]);
        }

        int before = fdDir.list().length;
        for (int i = 0; i < 200; i++) {
            assertThatThrownBy(
                            () ->
                                    new LuminaVectorGlobalIndexWriter.FileBackedDataset(
                                            file, Integer.MAX_VALUE / 4, 1L, "test", 4096))
                    .isInstanceOf(IllegalStateException.class);
        }
        int after = fdDir.list().length;

        assertThat(after - before)
                .as("200 failed constructions must not strand 200 descriptors")
                .isLessThan(50);
    }
}
