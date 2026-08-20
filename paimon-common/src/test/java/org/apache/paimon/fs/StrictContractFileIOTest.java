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

package org.apache.paimon.fs;

import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StrictContractFileIO}. */
class StrictContractFileIOTest {

    @TempDir private java.nio.file.Path tempDir;

    private FileIO delegate;
    private StrictContractFileIO strict;
    private Path root;

    @BeforeEach
    void before() throws Exception {
        delegate = new LocalFileIO();
        strict = new StrictContractFileIO(delegate);
        root = new Path(tempDir.toUri());
        delegate.mkdirs(root);
    }

    @Test
    void testOverridesEveryFileIOInstanceMethod() throws Exception {
        for (Method method : FileIO.class.getDeclaredMethods()) {
            if (Modifier.isPublic(method.getModifiers())
                    && !Modifier.isStatic(method.getModifiers())
                    && !method.isSynthetic()) {
                assertThat(
                                StrictContractFileIO.class
                                        .getMethod(method.getName(), method.getParameterTypes())
                                        .getDeclaringClass())
                        .as(method.toString())
                        .isEqualTo(StrictContractFileIO.class);
            }
        }
    }

    @Test
    void testListingRequiresExistingDirectory() throws Exception {
        Path file = new Path(root, "file");
        delegate.writeFile(file, "content", false);
        Path missing = new Path(root, "missing");

        assertThat(strict.listStatus(root)).extracting(FileStatus::getPath).containsExactly(file);
        assertThatThrownBy(() -> strict.listStatus(file)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> strict.listFilesIterative(file, false))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> strict.listFiles(missing, true))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> strict.listDirectories(missing))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void testRenameAllowsExactMissingDestination() throws Exception {
        Path source = new Path(root, "source");
        Path destination = new Path(root, "destination");
        delegate.writeFile(source, "content", false);

        assertThat(strict.rename(source, destination)).isTrue();

        assertThat(delegate.exists(source)).isFalse();
        assertThat(delegate.readFileUtf8(destination)).isEqualTo("content");
    }

    @Test
    void testRenameRejectsUnspecifiedShapesBeforeMutation() throws Exception {
        Path source = new Path(root, "source");
        Path destination = new Path(root, "destination");
        delegate.writeFile(source, "source", false);
        delegate.writeFile(destination, "destination", false);

        assertThatThrownBy(() -> strict.rename(source, source)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> strict.rename(source, destination))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(
                        () ->
                                strict.rename(
                                        new Path(root, "missing-source"),
                                        new Path(root, "missing-destination")))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(
                        () ->
                                strict.rename(
                                        source,
                                        new Path(new Path(root, "missing-parent"), "target")))
                .isInstanceOf(AssertionError.class);

        Path fileParent = new Path(root, "file-parent");
        delegate.writeFile(fileParent, "not-a-directory", false);
        assertThatThrownBy(() -> strict.rename(source, new Path(fileParent, "target")))
                .isInstanceOf(AssertionError.class);

        assertThat(delegate.readFileUtf8(source)).isEqualTo("source");
        assertThat(delegate.readFileUtf8(destination)).isEqualTo("destination");
    }

    @Test
    void testCopyFilesRequiresExistingSourceDirectory() throws Exception {
        Path sourceDirectory = new Path(root, "source");
        Path targetDirectory = new Path(root, "target");
        delegate.mkdirs(sourceDirectory);
        delegate.mkdirs(targetDirectory);
        delegate.writeFile(new Path(sourceDirectory, "file"), "content", false);

        strict.copyFiles(sourceDirectory, targetDirectory, false);

        assertThat(delegate.readFileUtf8(new Path(targetDirectory, "file"))).isEqualTo("content");
        assertThatThrownBy(
                        () ->
                                strict.copyFiles(
                                        new Path(sourceDirectory, "file"), targetDirectory, false))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(
                        () ->
                                strict.copyFiles(
                                        new Path(root, "missing-source"), targetDirectory, false))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void testDocumentedErrorPathsAreForwarded() throws Exception {
        Path missing = new Path(root, "missing");
        Path existing = new Path(root, "existing");
        delegate.writeFile(existing, "old", false);

        assertThatThrownBy(() -> strict.getFileStatus(missing))
                .isInstanceOf(FileNotFoundException.class);
        assertThatThrownBy(() -> strict.writeFile(existing, "new", false))
                .isInstanceOf(Exception.class);
        assertThat(strict.tryToWriteAtomic(existing, "new")).isFalse();
        assertThat(delegate.readFileUtf8(existing)).isEqualTo("old");
        assertThatThrownBy(() -> strict.archive(existing, StorageType.ARCHIVE))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> strict.restoreArchive(existing, Duration.ofMinutes(1)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testTwoPhaseCommitUsesTheProviderWithoutReapplyingCallerGuards() throws Exception {
        Path target = new Path(root, "two-phase");
        delegate.writeFile(target, "old", false);

        TwoPhaseOutputStream output = strict.newTwoPhaseOutputStream(target, true);
        output.write("replacement".getBytes());
        TwoPhaseOutputStream.Committer committer = output.closeForCommit();
        committer.commit(strict);
        committer.clean(strict);

        assertThat(delegate.readFileUtf8(target)).isEqualTo("replacement");
    }
}
