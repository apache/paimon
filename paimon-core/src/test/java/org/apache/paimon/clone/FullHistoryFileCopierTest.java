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

package org.apache.paimon.clone;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FullHistoryFileCopier}. */
public class FullHistoryFileCopierTest {

    @TempDir private java.nio.file.Path tempDir;

    private final FileIO sourceFileIO = LocalFileIO.create();
    private final FileIO targetFileIO = LocalFileIO.create();

    @Test
    public void testCopyFilesByPlan() throws Exception {
        java.nio.file.Path sourceDir = tempDir.resolve("source");
        java.nio.file.Path targetDir = tempDir.resolve("target");
        Path source = new Path(sourceDir.resolve("db/t/schema/schema-0").toString());
        Path target = new Path(targetDir.resolve("db/t/schema/schema-0").toString());
        sourceFileIO.writeFile(source, "schema-content", false);
        FullHistoryCopyPlan plan =
                singleFilePlan(source, sourceDir.toString(), targetDir.toString());

        FullHistoryFileCopier.copy(sourceFileIO, targetFileIO, plan, false);

        assertThat(targetFileIO.readFileUtf8(target)).isEqualTo("schema-content");
    }

    @Test
    public void testCopyStreamsDirectlyToOwnedTarget() throws Exception {
        java.nio.file.Path sourceDir = tempDir.resolve("direct-source");
        java.nio.file.Path targetDir = tempDir.resolve("direct-target");
        Path source = new Path(sourceDir.resolve("data/file.orc").toString());
        Path target = new Path(targetDir.resolve("data/file.orc").toString());
        sourceFileIO.writeFile(source, "content", false);
        FullHistoryCopyPlan plan =
                singleFilePlan(source, sourceDir.toString(), targetDir.toString());
        DirectWriteFileIO directWriteFileIO = new DirectWriteFileIO();

        FullHistoryFileCopier.copy(sourceFileIO, directWriteFileIO, plan, false);

        assertThat(directWriteFileIO.outputStreamCalls).isEqualTo(1);
        assertThat(directWriteFileIO.twoPhaseOutputStreamCalls).isZero();
        assertThat(directWriteFileIO.readFileUtf8(target)).isEqualTo("content");
    }

    @Test
    public void testSkipExistingTargetWithSameSizeWhenOverwriteIsFalse() throws Exception {
        java.nio.file.Path sourceDir = tempDir.resolve("source");
        java.nio.file.Path targetDir = tempDir.resolve("target");
        Path source = new Path(sourceDir.resolve("db/t/data/file.orc").toString());
        Path target = new Path(targetDir.resolve("db/t/data/file.orc").toString());
        sourceFileIO.writeFile(source, "new-content", false);
        targetFileIO.writeFile(target, "old-content", false);
        FullHistoryCopyPlan plan =
                singleFilePlan(source, sourceDir.toString(), targetDir.toString());

        FullHistoryFileCopier.copy(sourceFileIO, targetFileIO, plan, false);

        assertThat(targetFileIO.readFileUtf8(target)).isEqualTo("old-content");
    }

    @Test
    public void testExistingTargetWithDifferentSizeFails() throws Exception {
        java.nio.file.Path sourceDir = tempDir.resolve("source");
        java.nio.file.Path targetDir = tempDir.resolve("target");
        Path source = new Path(sourceDir.resolve("db/t/data/file.orc").toString());
        Path target = new Path(targetDir.resolve("db/t/data/file.orc").toString());
        sourceFileIO.writeFile(source, "new-content", false);
        targetFileIO.writeFile(target, "short", false);
        FullHistoryCopyPlan plan =
                singleFilePlan(source, sourceDir.toString(), targetDir.toString());

        assertThatThrownBy(
                        () -> FullHistoryFileCopier.copy(sourceFileIO, targetFileIO, plan, false))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("different size")
                .hasMessageContaining(target.toString());
    }

    @Test
    public void testOverwriteExistingTarget() throws Exception {
        java.nio.file.Path sourceDir = tempDir.resolve("source");
        java.nio.file.Path targetDir = tempDir.resolve("target");
        Path source = new Path(sourceDir.resolve("db/t/data/file.orc").toString());
        Path target = new Path(targetDir.resolve("db/t/data/file.orc").toString());
        sourceFileIO.writeFile(source, "new-content", false);
        targetFileIO.writeFile(target, "old-content", false);
        FullHistoryCopyPlan plan =
                singleFilePlan(source, sourceDir.toString(), targetDir.toString());

        FullHistoryFileCopier.copy(sourceFileIO, targetFileIO, plan, true);

        assertThat(targetFileIO.readFileUtf8(target)).isEqualTo("new-content");
    }

    @Test
    public void testFailedCopyDoesNotPublishPartialTarget() throws Exception {
        java.nio.file.Path sourceDir = tempDir.resolve("failing-source");
        java.nio.file.Path targetDir = tempDir.resolve("failing-target");
        FileIO failingSourceFileIO = new FailingReadFileIO();
        Path source = new Path(sourceDir.resolve("data/file.orc").toString());
        Path target = new Path(targetDir.resolve("data/file.orc").toString());
        sourceFileIO.writeFile(source, "content that must not become partially visible", false);
        FullHistoryFileSet.Builder fileSet = FullHistoryFileSet.builder();
        fileSet.addDataFile(source);
        FullHistoryCopyPlan plan =
                FullHistoryCopyPlan.buildPayload(
                        fileSet.build(),
                        PathMapping.parse(Collections.singletonList(sourceDir + "=" + targetDir)),
                        failingSourceFileIO);

        assertThatThrownBy(
                        () ->
                                FullHistoryFileCopier.copy(
                                        failingSourceFileIO, targetFileIO, plan, false))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("Injected read failure");
        assertThat(targetFileIO.exists(target)).isFalse();
    }

    private static class FailingReadFileIO extends LocalFileIO {

        @Override
        public SeekableInputStream newInputStream(Path path) throws java.io.IOException {
            return new SeekableInputStreamWrapper(super.newInputStream(path)) {

                private boolean firstRead = true;

                @Override
                public int read(byte[] bytes, int offset, int length) throws java.io.IOException {
                    if (!firstRead) {
                        throw new java.io.IOException("Injected read failure.");
                    }
                    firstRead = false;
                    return in.read(bytes, offset, Math.min(length, 4));
                }
            };
        }
    }

    private static class DirectWriteFileIO extends LocalFileIO {

        private int outputStreamCalls;
        private int twoPhaseOutputStreamCalls;

        @Override
        public org.apache.paimon.fs.PositionOutputStream newOutputStream(
                Path path, boolean overwrite) throws java.io.IOException {
            outputStreamCalls++;
            return super.newOutputStream(path, overwrite);
        }

        @Override
        public TwoPhaseOutputStream newTwoPhaseOutputStream(Path path, boolean overwrite)
                throws java.io.IOException {
            twoPhaseOutputStreamCalls++;
            throw new java.io.IOException("Two-phase output is not expected for clone payloads.");
        }
    }

    private FullHistoryCopyPlan singleFilePlan(
            Path source, String sourcePrefix, String targetPrefix) {
        FullHistoryFileSet.Builder builder = FullHistoryFileSet.builder();
        builder.addDataFile(source);
        try {
            return FullHistoryCopyPlan.buildPayload(
                    builder.build(),
                    PathMapping.parse(Collections.singletonList(sourcePrefix + "=" + targetPrefix)),
                    sourceFileIO);
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }
}
