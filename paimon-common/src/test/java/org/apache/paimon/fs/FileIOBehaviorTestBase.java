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

import org.apache.paimon.utils.StringUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Provider-neutral contract tests for {@link FileIO}. */
public abstract class FileIOBehaviorTestBase {

    private static final Random RND = new Random();

    private static final byte[] DEFAULT_CONTENT = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};

    /** The cached file system instance. */
    private FileIO fs;

    /** The cached base path. */
    private Path basePath;

    // ------------------------------------------------------------------------
    //  FileSystem-specific methods
    // ------------------------------------------------------------------------

    /** Gets an instance of the {@code FileSystem} to be tested. */
    protected abstract FileIO getFileSystem() throws Exception;

    /** Gets the base path in the file system under which tests will place their temporary files. */
    protected abstract Path getBasePath() throws Exception;

    // ------------------------------------------------------------------------
    //  Init / Cleanup
    // ------------------------------------------------------------------------

    @BeforeEach
    void prepare() throws Exception {
        fs = getFileSystem();
        basePath = new Path(getBasePath(), randomName());
        fs.mkdirs(basePath);
    }

    @AfterEach
    void cleanup() throws Exception {
        fs.delete(basePath, true);
    }

    @Test
    void testObjectStoreClassificationIsStable() throws IOException {
        boolean objectStore = fs.isObjectStore();
        Path file = createRandomFileInDirectory(basePath);

        assertThat(fs.isObjectStore()).isEqualTo(objectStore);

        fs.delete(file, false);
        assertThat(fs.isObjectStore()).isEqualTo(objectStore);
    }

    // ------------------------------------------------------------------------
    //  Input streams
    // ------------------------------------------------------------------------

    @Test
    void testInputStreamStartsAtZeroAndReadsCorrectBytes() throws IOException {
        byte[] content = new byte[] {3, 1, 4, 1, 5, 9};
        Path file = createRandomFileInDirectory(basePath, content);

        try (SeekableInputStream in = fs.newInputStream(file)) {
            assertThat(in.getPos()).isZero();
            assertThat(readAll(in)).containsExactly(content);
            assertThat(in.getPos()).isEqualTo(content.length);
        }
    }

    @Test
    void testInputStreamBulkReadHonorsNonZeroBufferOffset() throws IOException {
        byte[] content = new byte[] {11, 22, 33};
        Path file = createRandomFileInDirectory(basePath, content);
        byte[] buffer = new byte[] {99, 98, 0, 0, 0, 97, 96};

        try (SeekableInputStream in = fs.newInputStream(file)) {
            int totalRead = 0;
            while (totalRead < content.length) {
                int read = in.read(buffer, 2 + totalRead, content.length - totalRead);
                assertThat(read).isPositive();
                totalRead += read;
            }

            assertThat(totalRead).isEqualTo(content.length);
            assertThat(buffer).containsExactly(99, 98, 11, 22, 33, 97, 96);
            assertThat(in.getPos()).isEqualTo(content.length);
        }
    }

    @Test
    void testInputStreamsHaveIndependentPositions() throws IOException {
        Path file = createRandomFileInDirectory(basePath, new byte[] {10, 20, 30});

        try (SeekableInputStream first = fs.newInputStream(file);
                SeekableInputStream second = fs.newInputStream(file)) {
            assertThat(first.read()).isEqualTo(10);
            assertThat(first.getPos()).isEqualTo(1);
            assertThat(second.getPos()).isZero();
            assertThat(second.read()).isEqualTo(10);
            assertThat(second.getPos()).isEqualTo(1);

            first.seek(2);
            assertThat(first.read()).isEqualTo(30);
            assertThat(second.read()).isEqualTo(20);
        }
    }

    @Test
    void testInputStreamSeeksForwardAndBackward() throws IOException {
        byte[] content = new byte[] {10, 20, 30, 40, 50, 60};
        Path file = createRandomFileInDirectory(basePath, content);

        try (SeekableInputStream in = fs.newInputStream(file)) {
            in.seek(4);
            assertThat(in.getPos()).isEqualTo(4);
            assertThat(in.read()).isEqualTo(50);

            in.seek(1);
            assertThat(in.getPos()).isEqualTo(1);
            assertThat(in.read()).isEqualTo(20);
        }
    }

    @Test
    void testInputStreamReturnsEndOfFileAtFileLength() throws IOException {
        byte[] content = new byte[] {7, 8, 9};
        Path file = createRandomFileInDirectory(basePath, content);

        try (SeekableInputStream in = fs.newInputStream(file)) {
            in.seek(content.length);
            assertThat(in.read()).isEqualTo(-1);
            assertThat(in.read(new byte[2], 0, 2)).isEqualTo(-1);
        }
    }

    @Test
    void testInputStreamCanSeekBackToStart() throws IOException {
        byte[] content = new byte[] {7, 8, 9};
        Path file = createRandomFileInDirectory(basePath, content);

        try (SeekableInputStream in = fs.newInputStream(file)) {
            assertThat(in.read()).isEqualTo(7);
            in.seek(0);
            assertThat(in.getPos()).isZero();
            assertThat(in.read()).isEqualTo(7);
        }
    }

    @Test
    void testInputStreamSeeksForwardBeyondOneMebibyte() throws IOException {
        int targetPosition = 1024 * 1024 + 17;
        byte[] content = new byte[targetPosition + 1];
        content[targetPosition] = 42;
        Path file = createRandomFileInDirectory(basePath, content);

        try (SeekableInputStream in = fs.newInputStream(file)) {
            in.seek(targetPosition);
            assertThat(in.getPos()).isEqualTo(targetPosition);
            assertThat(in.read()).isEqualTo(42);
        }
    }

    @Test
    void testInputStreamForMissingFileFailsByFirstRead() {
        Path missing = new Path(basePath, randomName());

        assertOpenOrFirstReadFails(missing);
    }

    @Test
    void testInputStreamForDirectoryFailsByFirstRead() throws IOException {
        Path directory = new Path(basePath, randomName());
        fs.mkdirs(directory);

        assertOpenOrFirstReadFails(directory);
    }

    // ------------------------------------------------------------------------
    //  Output streams
    // ------------------------------------------------------------------------

    @Test
    void testOutputStreamTracksPositionAndPublishesBytesOnClose() throws IOException {
        Path file = new Path(basePath, randomName());

        try (PositionOutputStream out = fs.newOutputStream(file, false)) {
            assertThat(out.getPos()).isZero();
            out.write(9);
            assertThat(out.getPos()).isEqualTo(1);
            out.write(new byte[] {10, 11, 12, 13}, 1, 2);
            assertThat(out.getPos()).isEqualTo(3);
        }

        assertThat(readBytes(file)).containsExactly(9, 11, 12);
    }

    @Test
    void testOutputStreamCreatesNestedTarget() throws IOException {
        Path ancestor = new Path(basePath, randomName());
        Path parent = new Path(ancestor, randomName());
        Path file = new Path(parent, randomName());
        byte[] content = new byte[] {1, 3, 3, 7};

        writeBytes(file, content, false);

        assertThat(readBytes(file)).containsExactly(content);
        assertThat(fs.getFileStatus(ancestor).isDir()).isTrue();
        assertThat(fs.getFileStatus(parent).isDir()).isTrue();
    }

    @Test
    void testOutputStreamOverwriteReplacesOldContent() throws IOException {
        Path file = createRandomFileInDirectory(basePath, new byte[] {1, 2, 3, 4, 5});

        writeBytes(file, new byte[] {8, 9}, true);

        assertThat(readBytes(file)).containsExactly(8, 9);
    }

    @Test
    void testOutputStreamNoOverwriteFailsAndPreservesOldContent() throws IOException {
        byte[] oldContent = new byte[] {1, 2, 3};
        Path file = createRandomFileInDirectory(basePath, oldContent);

        assertThatThrownBy(() -> writeBytes(file, new byte[] {9, 8, 7}, false))
                .isInstanceOf(IOException.class);
        assertThat(readBytes(file)).containsExactly(oldContent);
    }

    // ------------------------------------------------------------------------
    //  File status and existence
    // ------------------------------------------------------------------------

    @Test
    void testGetFileStatusForMissingPathThrowsFileNotFound() {
        Path missing = new Path(basePath, randomName());

        assertThatThrownBy(() -> fs.getFileStatus(missing))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testGetFileStatusDescribesFile() throws IOException {
        byte[] content = new byte[] {2, 4, 6, 8, 10};
        Path file = createRandomFileInDirectory(basePath, content);

        FileStatus status = fs.getFileStatus(file);

        assertThat(status.getPath()).isEqualTo(file);
        assertThat(status.isDir()).isFalse();
        assertThat(status.getLen()).isEqualTo(content.length);
    }

    @Test
    void testGetFileStatusDescribesDirectory() throws IOException {
        Path directory = new Path(basePath, randomName());
        fs.mkdirs(directory);

        FileStatus status = fs.getFileStatus(directory);

        assertThat(status.getPath()).isEqualTo(directory);
        assertThat(status.isDir()).isTrue();
    }

    @Test
    void testFileStatusIsSnapshot() throws IOException {
        Path path = createRandomFileInDirectory(basePath, new byte[] {1, 2, 3});
        FileStatus snapshot = fs.getFileStatus(path);

        assertThat(fs.delete(path, false)).isTrue();
        assertThat(fs.mkdirs(path)).isTrue();
        FileStatus current = fs.getFileStatus(path);

        assertThat(snapshot.getPath()).isEqualTo(path);
        assertThat(snapshot.isDir()).isFalse();
        assertThat(snapshot.getLen()).isEqualTo(3);
        assertThat(current.getPath()).isEqualTo(path);
        assertThat(current.isDir()).isTrue();
    }

    @Test
    void testExistsReturnsTrueForFile() throws IOException {
        Path file = createRandomFileInDirectory(basePath);

        assertThat(fs.exists(file)).isTrue();
    }

    @Test
    void testExistsReturnsTrueForLogicalDirectory() throws IOException {
        Path directory = new Path(basePath, randomName());
        fs.mkdirs(directory);

        assertThat(fs.exists(directory)).isTrue();
    }

    @Test
    void testExistsReturnsFalseForMissingPath() throws IOException {
        assertThat(fs.exists(new Path(basePath, randomName()))).isFalse();
    }

    // ------------------------------------------------------------------------
    //  Listings
    // ------------------------------------------------------------------------

    @Test
    void testListStatusOfEmptyDirectoryReturnsNonNullEmptyArray() throws IOException {
        FileStatus[] statuses = fs.listStatus(basePath);

        assertThat(statuses).isNotNull().isEmpty();
    }

    @Test
    void testListStatusReturnsOnlyCorrectDirectChildren() throws IOException {
        byte[] firstContent = new byte[] {1, 2, 3, 4};
        byte[] secondContent = new byte[] {5, 6};
        Path firstFile = createRandomFileInDirectory(basePath, firstContent);
        Path secondFile = createRandomFileInDirectory(basePath, secondContent);
        Path firstDirectory = new Path(basePath, randomName());
        Path secondDirectory = new Path(basePath, randomName());
        Path nestedDirectory = new Path(firstDirectory, randomName());
        createRandomFileInDirectory(nestedDirectory, new byte[] {9});
        fs.mkdirs(secondDirectory);

        FileStatus[] statuses = fs.listStatus(basePath);

        assertThat(statuses).isNotNull().hasSize(4);
        assertThat(statuses)
                .extracting(FileStatus::getPath)
                .containsExactlyInAnyOrder(firstFile, secondFile, firstDirectory, secondDirectory);
        FileStatus firstFileStatus = statusFor(statuses, firstFile);
        assertThat(firstFileStatus.isDir()).isFalse();
        assertThat(firstFileStatus.getLen()).isEqualTo(firstContent.length);
        FileStatus secondFileStatus = statusFor(statuses, secondFile);
        assertThat(secondFileStatus.isDir()).isFalse();
        assertThat(secondFileStatus.getLen()).isEqualTo(secondContent.length);
        assertThat(statusFor(statuses, firstDirectory).isDir()).isTrue();
        assertThat(statusFor(statuses, secondDirectory).isDir()).isTrue();
    }

    @Test
    void testListFilesNonRecursiveReturnsOnlyDirectFilesAndMatchesIterator() throws IOException {
        Path firstDirectFile = createRandomFileInDirectory(basePath);
        Path secondDirectFile = createRandomFileInDirectory(basePath);
        Path directory = new Path(basePath, randomName());
        Path nestedFile = createRandomFileInDirectory(directory);

        FileStatus[] arrayResult = fs.listFiles(basePath, false);
        List<FileStatus> iteratorResult = collect(fs.listFilesIterative(basePath, false));

        assertThat(arrayResult).isNotNull();
        assertThat(arrayResult)
                .extracting(FileStatus::getPath)
                .containsExactlyInAnyOrder(firstDirectFile, secondDirectFile);
        assertThat(iteratorResult)
                .extracting(FileStatus::getPath)
                .containsExactlyInAnyOrder(firstDirectFile, secondDirectFile);
        assertThat(Arrays.stream(arrayResult).allMatch(status -> !status.isDir())).isTrue();
        assertThat(iteratorResult.stream().allMatch(status -> !status.isDir())).isTrue();
        assertThat(arrayResult).noneMatch(status -> status.getPath().equals(nestedFile));
    }

    @Test
    void testListFilesRecursiveReturnsAllFilesAndMatchesIterator() throws IOException {
        Path firstDirectFile = createRandomFileInDirectory(basePath);
        Path secondDirectFile = createRandomFileInDirectory(basePath);
        Path firstLevelDirectory = new Path(basePath, randomName());
        Path firstLevelFile = createRandomFileInDirectory(firstLevelDirectory);
        Path secondLevelDirectory = new Path(firstLevelDirectory, randomName());
        Path secondLevelFile = createRandomFileInDirectory(secondLevelDirectory);

        FileStatus[] arrayResult = fs.listFiles(basePath, true);
        List<FileStatus> iteratorResult = collect(fs.listFilesIterative(basePath, true));

        assertThat(arrayResult).isNotNull();
        assertThat(arrayResult)
                .extracting(FileStatus::getPath)
                .containsExactlyInAnyOrder(
                        firstDirectFile, secondDirectFile, firstLevelFile, secondLevelFile);
        assertThat(iteratorResult)
                .extracting(FileStatus::getPath)
                .containsExactlyInAnyOrder(
                        firstDirectFile, secondDirectFile, firstLevelFile, secondLevelFile);
        assertThat(Arrays.stream(arrayResult).allMatch(status -> !status.isDir())).isTrue();
        assertThat(iteratorResult.stream().allMatch(status -> !status.isDir())).isTrue();
    }

    @Test
    void testListDirectoriesReturnsOnlyDirectDirectories() throws IOException {
        createRandomFileInDirectory(basePath);
        Path firstDirectDirectory = new Path(basePath, randomName());
        Path secondDirectDirectory = new Path(basePath, randomName());
        Path nestedDirectory = new Path(firstDirectDirectory, randomName());
        fs.mkdirs(nestedDirectory);
        fs.mkdirs(secondDirectDirectory);

        FileStatus[] statuses = fs.listDirectories(basePath);

        assertThat(statuses).isNotNull().hasSize(2);
        assertThat(statuses)
                .extracting(FileStatus::getPath)
                .containsExactlyInAnyOrder(firstDirectDirectory, secondDirectDirectory);
        assertThat(Arrays.stream(statuses).allMatch(FileStatus::isDir)).isTrue();
    }

    // ------------------------------------------------------------------------
    //  Delete
    // ------------------------------------------------------------------------

    @Test
    void testExistingFileDeletion() throws IOException {
        Path file = createRandomFileInDirectory(basePath);

        assertThat(fs.delete(file, false)).isTrue();

        assertThat(fs.exists(file)).isFalse();
    }

    @Test
    void testExistingFileRecursiveDeletion() throws IOException {
        Path file = createRandomFileInDirectory(basePath);

        assertThat(fs.delete(file, true)).isTrue();

        assertThat(fs.exists(file)).isFalse();
    }

    @Test
    void testExistingEmptyDirectoryDeletion() throws IOException {
        Path directory = new Path(basePath, randomName());
        fs.mkdirs(directory);

        assertThat(fs.delete(directory, false)).isTrue();

        assertThat(fs.exists(directory)).isFalse();
    }

    @Test
    void testExistingEmptyDirectoryRecursiveDeletion() throws IOException {
        Path directory = new Path(basePath, randomName());
        fs.mkdirs(directory);

        assertThat(fs.delete(directory, true)).isTrue();

        assertThat(fs.exists(directory)).isFalse();
    }

    @Test
    void testNonEmptyDirectoryNonRecursiveDeletionFailsWithoutDamage() throws IOException {
        Path directory = new Path(basePath, randomName());
        Path file = createRandomFileInDirectory(directory);

        assertThatThrownBy(() -> fs.delete(directory, false)).isInstanceOf(IOException.class);
        assertThat(fs.exists(directory)).isTrue();
        assertThat(fs.exists(file)).isTrue();
    }

    @Test
    void testRecursiveDeletionRemovesEntireSubtree() throws IOException {
        Path directory = new Path(basePath, randomName());
        Path directFile = createRandomFileInDirectory(directory);
        Path nestedDirectory = new Path(directory, randomName());
        Path nestedFile = createRandomFileInDirectory(nestedDirectory);

        assertThat(fs.delete(directory, true)).isTrue();

        assertThat(fs.exists(directory)).isFalse();
        assertThat(fs.exists(directFile)).isFalse();
        assertThat(fs.exists(nestedDirectory)).isFalse();
        assertThat(fs.exists(nestedFile)).isFalse();
    }

    @Test
    void testMissingPathDeletionLeavesPathAbsent() throws IOException {
        Path missing = new Path(basePath, randomName());

        fs.delete(missing, false);

        assertThat(fs.exists(missing)).isFalse();
    }

    @Test
    void testMissingPathRecursiveDeletionLeavesPathAbsent() throws IOException {
        Path missing = new Path(basePath, randomName());

        fs.delete(missing, true);

        assertThat(fs.exists(missing)).isFalse();
    }

    // ------------------------------------------------------------------------
    //  Mkdirs
    // ------------------------------------------------------------------------

    @Test
    void testMkdirsCreatesTargetAndLogicalParents() throws IOException {
        Path first = new Path(basePath, randomName());
        Path second = new Path(first, randomName());
        Path target = new Path(second, randomName());

        assertThat(fs.mkdirs(target)).isTrue();
        assertThat(fs.getFileStatus(first).isDir()).isTrue();
        assertThat(fs.getFileStatus(second).isDir()).isTrue();
        assertThat(fs.getFileStatus(target).isDir()).isTrue();
    }

    @Test
    void testMkdirsReturnsTrueForExistingDirectory() throws IOException {
        Path directory = new Path(basePath, randomName());
        assertThat(fs.mkdirs(directory)).isTrue();

        assertThat(fs.mkdirs(directory)).isTrue();
        assertThat(fs.getFileStatus(directory).isDir()).isTrue();
    }

    // ------------------------------------------------------------------------
    //  Rename
    // ------------------------------------------------------------------------

    @Test
    void testRenameFileMovesExactBytesToMissingDestination() throws IOException {
        byte[] content = new byte[] {4, 2, 4, 2};
        Path source = createRandomFileInDirectory(basePath, content);
        Path destination = new Path(basePath, randomName());

        assertThat(fs.rename(source, destination)).isTrue();

        assertThat(fs.exists(source)).isFalse();
        assertThat(readBytes(destination)).containsExactly(content);
    }

    @Test
    void testRenameDirectoryMovesExactTreeToMissingDestination() throws IOException {
        Path source = new Path(basePath, randomName());
        Path child = createRandomFileInDirectory(source, new byte[] {1, 2});
        Path nestedDirectory = new Path(source, randomName());
        Path nestedChild = createRandomFileInDirectory(nestedDirectory, new byte[] {3, 4, 5});
        Path destination = new Path(basePath, randomName());

        assertThat(fs.rename(source, destination)).isTrue();

        assertThat(fs.exists(source)).isFalse();
        assertThat(fs.exists(child)).isFalse();
        assertThat(fs.exists(nestedDirectory)).isFalse();
        assertThat(fs.exists(nestedChild)).isFalse();
        assertThat(readBytes(new Path(destination, child.getName()))).containsExactly(1, 2);
        assertThat(
                        readBytes(
                                new Path(
                                        new Path(destination, nestedDirectory.getName()),
                                        nestedChild.getName())))
                .containsExactly(3, 4, 5);
    }

    // ------------------------------------------------------------------------
    //  Copy
    // ------------------------------------------------------------------------

    @Test
    void testCopyFileCreatesDestinationWithSourceBytes() throws IOException {
        byte[] content = new byte[] {6, 2, 6, 4, 3};
        Path source = createRandomFileInDirectory(basePath, content);
        Path destination = new Path(basePath, randomName());

        fs.copyFile(source, destination, false);

        assertThat(readBytes(destination)).containsExactly(content);
        assertThat(readBytes(source)).containsExactly(content);
    }

    @Test
    void testCopyFileOverwriteReplacesDestination() throws IOException {
        byte[] content = new byte[] {7, 7};
        Path source = createRandomFileInDirectory(basePath, content);
        Path destination = createRandomFileInDirectory(basePath, new byte[] {1, 2, 3, 4});

        fs.copyFile(source, destination, true);

        assertThat(readBytes(destination)).containsExactly(content);
        assertThat(readBytes(source)).containsExactly(content);
    }

    @Test
    void testCopyFileNoOverwriteFailsAndPreservesDestination() throws IOException {
        byte[] sourceContent = new byte[] {9, 9};
        Path source = createRandomFileInDirectory(basePath, sourceContent);
        byte[] destinationContent = new byte[] {1, 2, 3};
        Path destination = createRandomFileInDirectory(basePath, destinationContent);

        assertThatThrownBy(() -> fs.copyFile(source, destination, false))
                .isInstanceOf(IOException.class);
        assertThat(readBytes(destination)).containsExactly(destinationContent);
        assertThat(readBytes(source)).containsExactly(sourceContent);
    }

    @Test
    void testCopyFilesCopiesEveryDirectFile() throws IOException {
        Path sourceDirectory = new Path(basePath, randomName());
        Path first = createRandomFileInDirectory(sourceDirectory, new byte[] {1, 3});
        Path second = createRandomFileInDirectory(sourceDirectory, new byte[] {2, 4, 6});
        Path targetDirectory = new Path(basePath, randomName());
        fs.mkdirs(targetDirectory);

        fs.copyFiles(sourceDirectory, targetDirectory, false);

        assertThat(readBytes(new Path(targetDirectory, first.getName()))).containsExactly(1, 3);
        assertThat(readBytes(new Path(targetDirectory, second.getName()))).containsExactly(2, 4, 6);
        assertThat(readBytes(first)).containsExactly(1, 3);
        assertThat(readBytes(second)).containsExactly(2, 4, 6);
    }

    // ------------------------------------------------------------------------
    //  Two-phase output
    // ------------------------------------------------------------------------

    @Test
    void testTwoPhaseOutputPublishesOnlyAfterCommit() throws IOException {
        Path target = new Path(basePath, randomName());
        byte[] content = new byte[] {5, 4, 3, 2, 1};
        TwoPhaseOutputStream.Committer committer;
        try (TwoPhaseOutputStream out = fs.newTwoPhaseOutputStream(target, false)) {
            assertThat(out.getPos()).isZero();
            out.write(content);
            assertThat(out.getPos()).isEqualTo(content.length);
            assertThat(fs.exists(target)).isFalse();
            committer = out.closeForCommit();
        }

        assertThat(committer.targetPath()).isEqualTo(target);
        assertThat(fs.exists(target)).isFalse();

        committer.commit(fs);
        assertThat(readBytes(target)).containsExactly(content);
    }

    @Test
    void testTwoPhaseDiscardDoesNotPublishAbandonedData() throws IOException {
        Path target = new Path(basePath, randomName());
        TwoPhaseOutputStream.Committer committer;
        try (TwoPhaseOutputStream out = fs.newTwoPhaseOutputStream(target, false)) {
            out.write(new byte[] {1, 2, 3});
            committer = out.closeForCommit();
        }

        committer.discard(fs);

        assertThat(fs.exists(target)).isFalse();
    }

    @Test
    void testTwoPhaseDiscardPreservesPreExistingTarget() throws IOException {
        byte[] oldContent = new byte[] {8, 6, 7, 5};
        Path target = new Path(basePath, randomName());
        TwoPhaseOutputStream.Committer committer;
        try (TwoPhaseOutputStream out = fs.newTwoPhaseOutputStream(target, false)) {
            out.write(new byte[] {3, 0, 9});
            committer = out.closeForCommit();
        }
        assertThat(fs.exists(target)).isFalse();

        writeBytes(target, oldContent, false);
        assertThat(readBytes(target)).containsExactly(oldContent);

        committer.discard(fs);

        assertThat(fs.exists(target)).isTrue();
        assertThat(readBytes(target)).containsExactly(oldContent);
    }

    @Test
    void testTwoPhaseDiscardDoesNotAffectAnotherWriter() throws IOException {
        Path target = new Path(basePath, randomName());
        TwoPhaseOutputStream.Committer abandoned;
        try (TwoPhaseOutputStream out = fs.newTwoPhaseOutputStream(target, false)) {
            out.write(new byte[] {1, 1, 1});
            abandoned = out.closeForCommit();
        }

        byte[] committedContent = new byte[] {2, 2, 2};
        TwoPhaseOutputStream.Committer successful;
        try (TwoPhaseOutputStream out = fs.newTwoPhaseOutputStream(target, false)) {
            out.write(committedContent);
            successful = out.closeForCommit();
        }

        abandoned.discard(fs);
        successful.commit(fs);

        assertThat(readBytes(target)).containsExactly(committedContent);
    }

    @Test
    void testTwoPhaseCleanPreservesCommittedTarget() throws IOException {
        byte[] content = new byte[] {2, 7, 1, 8};
        Path target = new Path(basePath, randomName());
        TwoPhaseOutputStream.Committer committer;
        try (TwoPhaseOutputStream out = fs.newTwoPhaseOutputStream(target, false)) {
            out.write(content);
            committer = out.closeForCommit();
        }
        committer.commit(fs);

        committer.clean(fs);

        assertThat(readBytes(target)).containsExactly(content);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    protected static String randomName() {
        return StringUtils.getRandomString(RND, 16, 16, 'a', 'z');
    }

    private void writeBytes(Path file, byte[] content, boolean overwrite) throws IOException {
        try (PositionOutputStream out = fs.newOutputStream(file, overwrite)) {
            out.write(content);
        }
    }

    private byte[] readBytes(Path file) throws IOException {
        try (SeekableInputStream in = fs.newInputStream(file)) {
            return readAll(in);
        }
    }

    private void assertOpenOrFirstReadFails(Path path) {
        final SeekableInputStream in;
        try {
            in = fs.newInputStream(path);
        } catch (IOException expectedAtOpen) {
            return;
        }

        try {
            assertThatThrownBy(() -> in.read()).isInstanceOf(IOException.class);
        } finally {
            try {
                in.close();
            } catch (IOException ignoredAtClose) {
                // A close-only failure is deliberately irrelevant to the open/first-read contract.
            }
        }
    }

    private static byte[] readAll(SeekableInputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[4];
        int read;
        while ((read = in.read(buffer, 0, buffer.length)) != -1) {
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }

    private static List<FileStatus> collect(RemoteIterator<FileStatus> iterator)
            throws IOException {
        List<FileStatus> statuses = new ArrayList<>();
        while (iterator.hasNext()) {
            statuses.add(iterator.next());
        }
        return statuses;
    }

    private static FileStatus statusFor(FileStatus[] statuses, Path path) {
        for (FileStatus status : statuses) {
            if (status.getPath().equals(path)) {
                return status;
            }
        }
        throw new AssertionError("No status for " + path);
    }

    private Path createRandomFileInDirectory(Path directory) throws IOException {
        return createRandomFileInDirectory(directory, DEFAULT_CONTENT);
    }

    private Path createRandomFileInDirectory(Path directory, byte[] content) throws IOException {
        fs.mkdirs(directory);
        Path file = new Path(directory, randomName());
        writeBytes(file, content, false);
        return file;
    }
}
