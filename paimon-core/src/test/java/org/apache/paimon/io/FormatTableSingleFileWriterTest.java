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

package org.apache.paimon.io;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FormatTableSingleFileWriter}. */
public class FormatTableSingleFileWriterTest {

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private Path path;

    @BeforeEach
    public void beforeEach() {
        fileIO = LocalFileIO.create();
        path = new Path(tempDir.toString(), "data-0.orc");
    }

    @Test
    public void testRuntimeExceptionWhileOpeningLeavesNoFileBehind() throws IOException {
        assertThatThrownBy(
                        () ->
                                newWriter(
                                        (out, compression) -> {
                                            throw new IllegalArgumentException("bad compression");
                                        }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bad compression");

        // the two phase stream writes to a staging file, so no file may survive anywhere below
        assertThat(fileIO.listFiles(new Path(tempDir.toString()), true)).isEmpty();
    }

    @Test
    public void testIOExceptionWhileOpeningLeavesNoFileBehind() throws IOException {
        assertThatThrownBy(
                        () ->
                                newWriter(
                                        (out, compression) -> {
                                            throw new IOException("boom");
                                        }))
                .isInstanceOf(UncheckedIOException.class);

        assertThat(fileIO.listFiles(new Path(tempDir.toString()), true)).isEmpty();
    }

    @Test
    public void testExistingFileIsKeptWhenOpeningFails() throws IOException {
        fileIO.writeFile(path, "keep me", false);

        assertThatThrownBy(() -> newWriter((out, compression) -> new NoOpFormatWriter()))
                .isInstanceOf(UncheckedIOException.class);

        assertThat(fileIO.exists(path)).isTrue();
        assertThat(fileIO.readFileUtf8(path)).isEqualTo("keep me");
    }

    private FormatTableSingleFileWriter newWriter(FormatWriterFactory factory) {
        return new FormatTableSingleFileWriter(fileIO, factory, path, "zstd");
    }

    private static class NoOpFormatWriter implements FormatWriter {

        @Override
        public void addElement(InternalRow element) {}

        @Override
        public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
            return false;
        }

        @Override
        public void close() {}
    }
}
