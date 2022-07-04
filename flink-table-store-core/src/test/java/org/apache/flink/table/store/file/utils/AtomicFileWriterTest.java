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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.apache.flink.table.store.file.utils.AtomicFileWriter.writeFileUtf8;
import static org.apache.flink.table.store.file.utils.FileUtils.readFileUtf8;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AtomicFileWriter}. */
public class AtomicFileWriterTest {

    @TempDir java.nio.file.Path tempDir;

    private Path root;

    @BeforeEach
    public void beforeEach() {
        root = new Path(TestAtomicRenameFileSystem.SCHEME + "://" + tempDir.toString());
    }

    @Test
    public void testDefault() throws IOException {
        AtomicFileWriter writer = AtomicFileWriter.create(root.getFileSystem());
        Path path1 = new Path(root, "f1");
        boolean success = writeFileUtf8(writer, path1, "hahaha");
        assertThat(success).isTrue();
        assertThat(readFileUtf8(path1)).isEqualTo("hahaha");

        success = writeFileUtf8(writer, path1, "xixixi");
        assertThat(success).isFalse();
    }
}
