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

import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Opt-in provider-neutral contract tests for {@link FileIO}. */
public abstract class FileIOContractTestBase extends FileIOBehaviorTestBase {

    private static final byte[] DEFAULT_CONTENT = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};

    private FileIO contractFileIO;

    private Path contractBasePath;

    @AfterEach
    void cleanupContractFixture() throws IOException {
        if (contractFileIO != null) {
            contractFileIO.delete(contractBasePath, true);
        }
    }

    // ------------------------------------------------------------------------
    //  Input streams
    // ------------------------------------------------------------------------

    @Test
    void testInputStreamStartsAtZeroAndReadsCorrectBytes() throws IOException {
        byte[] content = new byte[] {3, 1, 4, 1, 5, 9};
        Path file = createRandomFileInDirectory(contractBasePath(), content);

        try (SeekableInputStream in = contractFileIO().newInputStream(file)) {
            assertThat(in.getPos()).isZero();
            assertThat(readAll(in)).containsExactly(content);
            assertThat(in.getPos()).isEqualTo(content.length);
        }
    }

    @Test
    void testInputStreamBulkReadHonorsNonZeroBufferOffset() throws IOException {
        byte[] content = new byte[] {11, 22, 33};
        Path file = createRandomFileInDirectory(contractBasePath(), content);
        byte[] buffer = new byte[] {99, 98, 0, 0, 0, 97, 96};

        try (SeekableInputStream in = contractFileIO().newInputStream(file)) {
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
        Path file = createRandomFileInDirectory(contractBasePath(), new byte[] {10, 20, 30});

        try (SeekableInputStream first = contractFileIO().newInputStream(file);
                SeekableInputStream second = contractFileIO().newInputStream(file)) {
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
        Path file = createRandomFileInDirectory(contractBasePath(), content);

        try (SeekableInputStream in = contractFileIO().newInputStream(file)) {
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
        Path file = createRandomFileInDirectory(contractBasePath(), content);

        try (SeekableInputStream in = contractFileIO().newInputStream(file)) {
            in.seek(content.length);
            assertThat(in.read()).isEqualTo(-1);
            assertThat(in.getPos()).isEqualTo(content.length);
            assertThat(in.read(new byte[2], 0, 2)).isEqualTo(-1);
            assertThat(in.getPos()).isEqualTo(content.length);
        }
    }

    @Test
    void testInputStreamCanSeekBackToStart() throws IOException {
        byte[] content = new byte[] {7, 8, 9};
        Path file = createRandomFileInDirectory(contractBasePath(), content);

        try (SeekableInputStream in = contractFileIO().newInputStream(file)) {
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
        Path file = createRandomFileInDirectory(contractBasePath(), content);

        try (SeekableInputStream in = contractFileIO().newInputStream(file)) {
            in.seek(targetPosition);
            assertThat(in.getPos()).isEqualTo(targetPosition);
            assertThat(in.read()).isEqualTo(42);
        }
    }

    @Test
    void testInputStreamForMissingFileFailsByFirstRead() throws IOException {
        Path missing = new Path(contractBasePath(), randomName());

        assertOpenOrFirstReadFails(missing);
    }

    @Test
    void testInputStreamForDirectoryFailsByFirstRead() throws IOException {
        Path directory = new Path(contractBasePath(), randomName());
        contractFileIO().mkdirs(directory);

        assertOpenOrFirstReadFails(directory);
    }

    // ------------------------------------------------------------------------
    //  Output streams
    // ------------------------------------------------------------------------

    @Test
    void testOutputStreamTracksPositionAndPublishesBytesOnClose() throws IOException {
        Path file = new Path(contractBasePath(), randomName());

        try (PositionOutputStream out = contractFileIO().newOutputStream(file, false)) {
            assertThat(out.getPos()).isZero();
            out.write(9);
            assertThat(out.getPos()).isEqualTo(1);
            out.write(new byte[] {10, 11, 12, 13}, 1, 2);
            assertThat(out.getPos()).isEqualTo(3);
        }

        assertThat(readBytes(file)).containsExactly(9, 11, 12);
    }

    @Test
    void testOutputStreamFlushKeepsPositionAndClosePublishesLaterWrites() throws IOException {
        Path file = new Path(contractBasePath(), randomName());

        try (PositionOutputStream out = contractFileIO().newOutputStream(file, false)) {
            out.write(new byte[] {1, 2});
            out.flush();
            assertThat(out.getPos()).isEqualTo(2);
            out.write(3);
            assertThat(out.getPos()).isEqualTo(3);
        }

        assertThat(readBytes(file)).containsExactly(1, 2, 3);
    }

    @Test
    void testOutputStreamCreatesNestedTarget() throws IOException {
        Path ancestor = new Path(contractBasePath(), randomName());
        Path parent = new Path(ancestor, randomName());
        Path file = new Path(parent, randomName());
        byte[] content = new byte[] {1, 3, 3, 7};

        writeBytes(file, content, false);

        assertThat(readBytes(file)).containsExactly(content);
        assertThat(contractFileIO().getFileStatus(ancestor).isDir()).isTrue();
        assertThat(contractFileIO().getFileStatus(parent).isDir()).isTrue();
    }

    @Test
    void testOutputStreamOverwriteReplacesOldContent() throws IOException {
        Path file = createRandomFileInDirectory(contractBasePath(), new byte[] {1, 2, 3, 4, 5});

        writeBytes(file, new byte[] {8, 9}, true);

        assertThat(readBytes(file)).containsExactly(8, 9);
    }

    @Test
    void testOutputStreamNoOverwriteFailsAndPreservesOldContent() throws IOException {
        byte[] oldContent = new byte[] {1, 2, 3};
        Path file = createRandomFileInDirectory(contractBasePath(), oldContent);

        assertThatThrownBy(() -> writeBytes(file, new byte[] {9, 8, 7}, false))
                .isInstanceOf(IOException.class);
        assertThat(readBytes(file)).containsExactly(oldContent);
    }

    // ------------------------------------------------------------------------
    //  File status
    // ------------------------------------------------------------------------

    @Test
    void testGetFileStatusForMissingPathThrowsFileNotFound() throws IOException {
        Path missing = new Path(contractBasePath(), randomName());

        assertThatThrownBy(() -> contractFileIO().getFileStatus(missing))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testGetFileStatusDescribesFile() throws IOException {
        byte[] content = new byte[] {2, 4, 6, 8, 10};
        Path file = createRandomFileInDirectory(contractBasePath(), content);

        FileStatus status = contractFileIO().getFileStatus(file);

        assertThat(status.getPath()).isEqualTo(file);
        assertThat(status.isDir()).isFalse();
        assertThat(status.getLen()).isEqualTo(content.length);
    }

    @Test
    void testGetFileStatusDescribesDirectory() throws IOException {
        Path directory = new Path(contractBasePath(), randomName());
        contractFileIO().mkdirs(directory);

        FileStatus status = contractFileIO().getFileStatus(directory);

        assertThat(status.getPath()).isEqualTo(directory);
        assertThat(status.isDir()).isTrue();
    }

    @Test
    void testFileStatusProvidesConsistentModificationTime() throws IOException {
        Path file = createRandomFileInDirectory(contractBasePath());

        FileStatus directStatus = contractFileIO().getFileStatus(file);
        FileStatus listedStatus = statusFor(contractFileIO().listStatus(contractBasePath()), file);

        long directModificationTime = directStatus.getModificationTime();
        long listedModificationTime = listedStatus.getModificationTime();
        assertThat(directModificationTime).isGreaterThan(1_000_000_000_000L);
        assertThat(listedModificationTime).isGreaterThan(1_000_000_000_000L);
        assertThat(Math.abs(listedModificationTime - directModificationTime))
                .isLessThanOrEqualTo(1_000L);
    }

    @Test
    void testExistsRecognizesDirectory() throws IOException {
        Path directory = new Path(contractBasePath(), randomName());
        contractFileIO().mkdirs(directory);

        assertThat(contractFileIO().exists(directory)).isTrue();
    }

    @Test
    void testStatusHelpersDescribeFilesAndDirectories() throws IOException {
        byte[] content = new byte[] {1, 4, 9, 16};
        Path file = createRandomFileInDirectory(contractBasePath(), content);

        assertThat(contractFileIO().getFileSize(file)).isEqualTo(content.length);
        assertThat(contractFileIO().isDir(file)).isFalse();
        assertThat(contractFileIO().isDir(contractBasePath())).isTrue();
    }

    // ------------------------------------------------------------------------
    //  Listings
    // ------------------------------------------------------------------------

    @Test
    void testListStatusOfEmptyDirectoryReturnsNonNullEmptyArray() throws IOException {
        FileStatus[] statuses = contractFileIO().listStatus(contractBasePath());

        assertThat(statuses).isEmpty();
    }

    @Test
    void testListStatusReturnsOnlyCorrectDirectChildren() throws IOException {
        byte[] firstContent = new byte[] {1, 2, 3, 4};
        byte[] secondContent = new byte[] {5, 6};
        Path firstFile = createRandomFileInDirectory(contractBasePath(), firstContent);
        Path secondFile = createRandomFileInDirectory(contractBasePath(), secondContent);
        Path firstDirectory = new Path(contractBasePath(), randomName());
        Path secondDirectory = new Path(contractBasePath(), randomName());
        Path nestedDirectory = new Path(firstDirectory, randomName());
        createRandomFileInDirectory(nestedDirectory, new byte[] {9});
        contractFileIO().mkdirs(secondDirectory);

        FileStatus[] statuses = contractFileIO().listStatus(contractBasePath());

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
        Path firstDirectFile = createRandomFileInDirectory(contractBasePath());
        Path secondDirectFile = createRandomFileInDirectory(contractBasePath());
        Path directory = new Path(contractBasePath(), randomName());
        createRandomFileInDirectory(directory);

        FileStatus[] arrayResult = contractFileIO().listFiles(contractBasePath(), false);
        List<FileStatus> iteratorResult =
                collect(contractFileIO().listFilesIterative(contractBasePath(), false));

        assertThat(arrayResult)
                .extracting(FileStatus::getPath)
                .containsExactlyInAnyOrder(firstDirectFile, secondDirectFile);
        assertThat(iteratorResult)
                .extracting(FileStatus::getPath)
                .containsExactlyInAnyOrder(firstDirectFile, secondDirectFile);
        assertThat(Arrays.stream(arrayResult).allMatch(status -> !status.isDir())).isTrue();
        assertThat(iteratorResult.stream().allMatch(status -> !status.isDir())).isTrue();
    }

    @Test
    void testListFilesRecursiveReturnsAllFilesAndMatchesIterator() throws IOException {
        Path firstDirectFile = createRandomFileInDirectory(contractBasePath());
        Path secondDirectFile = createRandomFileInDirectory(contractBasePath());
        Path firstLevelDirectory = new Path(contractBasePath(), randomName());
        Path firstLevelFile = createRandomFileInDirectory(firstLevelDirectory);
        Path secondLevelDirectory = new Path(firstLevelDirectory, randomName());
        Path secondLevelFile = createRandomFileInDirectory(secondLevelDirectory);

        FileStatus[] arrayResult = contractFileIO().listFiles(contractBasePath(), true);
        List<FileStatus> iteratorResult =
                collect(contractFileIO().listFilesIterative(contractBasePath(), true));

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
        createRandomFileInDirectory(contractBasePath());
        Path firstDirectDirectory = new Path(contractBasePath(), randomName());
        Path secondDirectDirectory = new Path(contractBasePath(), randomName());
        Path nestedDirectory = new Path(firstDirectDirectory, randomName());
        contractFileIO().mkdirs(nestedDirectory);
        contractFileIO().mkdirs(secondDirectDirectory);

        FileStatus[] statuses = contractFileIO().listDirectories(contractBasePath());

        assertThat(statuses)
                .extracting(FileStatus::getPath)
                .containsExactlyInAnyOrder(firstDirectDirectory, secondDirectDirectory);
        assertThat(Arrays.stream(statuses).allMatch(FileStatus::isDir)).isTrue();
    }

    // ------------------------------------------------------------------------
    //  Delete
    // ------------------------------------------------------------------------

    @Test
    void testDeleteReturnsTrueForExistingTargets() throws IOException {
        Path file = createRandomFileInDirectory(contractBasePath());
        Path directory = new Path(contractBasePath(), randomName());
        contractFileIO().mkdirs(directory);

        assertThat(contractFileIO().delete(file, false)).isTrue();
        assertThat(contractFileIO().delete(directory, false)).isTrue();
    }

    // ------------------------------------------------------------------------
    //  Rename
    // ------------------------------------------------------------------------

    @Test
    void testRenameFileMovesExactBytesToMissingDestination() throws IOException {
        byte[] content = new byte[] {4, 2, 4, 2};
        Path source = createRandomFileInDirectory(contractBasePath(), content);
        Path destination = new Path(contractBasePath(), randomName());

        assertThat(contractFileIO().rename(source, destination)).isTrue();

        assertThat(contractFileIO().exists(source)).isFalse();
        assertThat(readBytes(destination)).containsExactly(content);
    }

    @Test
    void testRenameDirectoryMovesExactTreeToMissingDestination() throws IOException {
        Path source = new Path(contractBasePath(), randomName());
        Path child = createRandomFileInDirectory(source, new byte[] {1, 2});
        Path nestedDirectory = new Path(source, randomName());
        Path nestedChild = createRandomFileInDirectory(nestedDirectory, new byte[] {3, 4, 5});
        Path destination = new Path(contractBasePath(), randomName());

        assertThat(contractFileIO().rename(source, destination)).isTrue();

        assertThat(contractFileIO().exists(source)).isFalse();
        assertThat(contractFileIO().exists(child)).isFalse();
        assertThat(contractFileIO().exists(nestedDirectory)).isFalse();
        assertThat(contractFileIO().exists(nestedChild)).isFalse();
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
        Path source = createRandomFileInDirectory(contractBasePath(), content);
        Path destination = new Path(contractBasePath(), randomName());

        contractFileIO().copyFile(source, destination, false);

        assertThat(readBytes(destination)).containsExactly(content);
        assertThat(readBytes(source)).containsExactly(content);
    }

    @Test
    void testCopyFileOverwriteReplacesDestination() throws IOException {
        byte[] content = new byte[] {7, 7};
        Path source = createRandomFileInDirectory(contractBasePath(), content);
        Path destination = createRandomFileInDirectory(contractBasePath(), new byte[] {1, 2, 3, 4});

        contractFileIO().copyFile(source, destination, true);

        assertThat(readBytes(destination)).containsExactly(content);
        assertThat(readBytes(source)).containsExactly(content);
    }

    @Test
    void testCopyFileNoOverwriteFailsAndPreservesDestination() throws IOException {
        byte[] sourceContent = new byte[] {9, 9};
        Path source = createRandomFileInDirectory(contractBasePath(), sourceContent);
        byte[] destinationContent = new byte[] {1, 2, 3};
        Path destination = createRandomFileInDirectory(contractBasePath(), destinationContent);

        assertThatThrownBy(() -> contractFileIO().copyFile(source, destination, false))
                .isInstanceOf(IOException.class);
        assertThat(readBytes(destination)).containsExactly(destinationContent);
        assertThat(readBytes(source)).containsExactly(sourceContent);
    }

    @Test
    void testCopyFilesCopiesEveryDirectFile() throws IOException {
        Path sourceDirectory = new Path(contractBasePath(), randomName());
        Path first = createRandomFileInDirectory(sourceDirectory, new byte[] {1, 3});
        Path second = createRandomFileInDirectory(sourceDirectory, new byte[] {2, 4, 6});
        Path targetDirectory = new Path(contractBasePath(), randomName());
        contractFileIO().mkdirs(targetDirectory);

        contractFileIO().copyFiles(sourceDirectory, targetDirectory, false);

        assertThat(readBytes(new Path(targetDirectory, first.getName()))).containsExactly(1, 3);
        assertThat(readBytes(new Path(targetDirectory, second.getName()))).containsExactly(2, 4, 6);
        assertThat(readBytes(first)).containsExactly(1, 3);
        assertThat(readBytes(second)).containsExactly(2, 4, 6);
    }

    // ------------------------------------------------------------------------
    //  Text and atomic helpers
    // ------------------------------------------------------------------------

    @Test
    void testUtf8ReadWriteHelpersPreserveContent() throws IOException {
        Path file = new Path(contractBasePath(), randomName());
        String content = "Paimon-文件-IO";

        contractFileIO().writeFile(file, content, false);

        assertThat(contractFileIO().readFileUtf8(file)).isEqualTo(content);
        assertThat(readBytes(file)).containsExactly(content.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void testOverwriteHelpersReplaceVisibleContent() throws IOException {
        Path file = new Path(contractBasePath(), randomName());
        contractFileIO().writeFile(file, "old", false);

        contractFileIO().overwriteFileUtf8(file, "new");
        assertThat(contractFileIO().readFileUtf8(file)).isEqualTo("new");

        contractFileIO().overwriteHintFile(file, "hint");
        assertThat(contractFileIO().readFileUtf8(file)).isEqualTo("hint");
    }

    @Test
    void testTryToWriteAtomicPublishesMissingTarget() throws IOException {
        Path target = new Path(contractBasePath(), randomName());

        assertThat(contractFileIO().tryToWriteAtomic(target, "atomic")).isTrue();
        assertThat(contractFileIO().readFileUtf8(target)).isEqualTo("atomic");
    }

    @Test
    void testTryToWriteAtomicPreservesExistingTarget() throws IOException {
        Path target = new Path(contractBasePath(), randomName());
        contractFileIO().writeFile(target, "existing", false);

        assertThat(contractFileIO().tryToWriteAtomic(target, "replacement")).isFalse();
        assertThat(contractFileIO().readFileUtf8(target)).isEqualTo("existing");
    }

    // ------------------------------------------------------------------------
    //  Two-phase output
    // ------------------------------------------------------------------------

    @Test
    void testTwoPhaseOutputPublishesOnlyAfterCommit() throws IOException {
        Path target = new Path(contractBasePath(), randomName());
        byte[] content = new byte[] {5, 4, 3, 2, 1};
        TwoPhaseOutputStream out = contractFileIO().newTwoPhaseOutputStream(target, false);
        assertThat(out.getPos()).isZero();
        out.write(content);
        assertThat(out.getPos()).isEqualTo(content.length);
        assertThat(contractFileIO().exists(target)).isFalse();
        TwoPhaseOutputStream.Committer committer = out.closeForCommit();

        assertThat(committer.targetPath()).isEqualTo(target);
        assertThat(contractFileIO().exists(target)).isFalse();

        committer.commit(contractFileIO());
        assertThat(readBytes(target)).containsExactly(content);
    }

    @Test
    void testTwoPhaseNoOverwritePreservesExistingTarget() throws IOException {
        Path target = new Path(contractBasePath(), randomName());
        byte[] existing = new byte[] {9, 8, 7};
        writeBytes(target, existing, false);

        AtomicReference<TwoPhaseOutputStream.Committer> staged = new AtomicReference<>();
        try {
            assertThatThrownBy(
                            () -> {
                                TwoPhaseOutputStream out =
                                        contractFileIO().newTwoPhaseOutputStream(target, false);
                                out.write(new byte[] {1, 2, 3});
                                staged.set(out.closeForCommit());
                                staged.get().commit(contractFileIO());
                            })
                    .isInstanceOf(IOException.class);
        } finally {
            if (staged.get() != null) {
                staged.get().discard(contractFileIO());
            }
        }
        assertThat(readBytes(target)).containsExactly(existing);
    }

    @Test
    void testTwoPhaseOverwriteReplacesExistingTargetOnCommit() throws IOException {
        Path target = new Path(contractBasePath(), randomName());
        writeBytes(target, new byte[] {9, 8, 7}, false);
        byte[] replacement = new byte[] {1, 2, 3};

        TwoPhaseOutputStream out = contractFileIO().newTwoPhaseOutputStream(target, true);
        out.write(replacement);
        out.closeForCommit().commit(contractFileIO());

        assertThat(readBytes(target)).containsExactly(replacement);
    }

    @Test
    void testTwoPhaseDiscardDoesNotPublishAbandonedData() throws IOException {
        Path target = new Path(contractBasePath(), randomName());
        TwoPhaseOutputStream.Committer committer =
                stageTwoPhaseOutput(target, new byte[] {1, 2, 3});

        committer.discard(contractFileIO());

        assertThat(contractFileIO().exists(target)).isFalse();
    }

    @Test
    void testTwoPhaseDiscardPreservesPreExistingTarget() throws IOException {
        byte[] oldContent = new byte[] {8, 6, 7, 5};
        Path target = new Path(contractBasePath(), randomName());
        TwoPhaseOutputStream.Committer committer =
                stageTwoPhaseOutput(target, new byte[] {3, 0, 9});
        assertThat(contractFileIO().exists(target)).isFalse();

        writeBytes(target, oldContent, false);
        assertThat(readBytes(target)).containsExactly(oldContent);

        committer.discard(contractFileIO());

        assertThat(readBytes(target)).containsExactly(oldContent);
    }

    @Test
    void testTwoPhaseDiscardDoesNotAffectAnotherWriter() throws IOException {
        Path target = new Path(contractBasePath(), randomName());
        TwoPhaseOutputStream.Committer abandoned =
                stageTwoPhaseOutput(target, new byte[] {1, 1, 1});

        byte[] committedContent = new byte[] {2, 2, 2};
        TwoPhaseOutputStream.Committer successful = stageTwoPhaseOutput(target, committedContent);

        abandoned.discard(contractFileIO());
        successful.commit(contractFileIO());

        assertThat(readBytes(target)).containsExactly(committedContent);
    }

    @Test
    void testTwoPhaseCleanPreservesCommittedTarget() throws IOException {
        byte[] content = new byte[] {2, 7, 1, 8};
        Path target = new Path(contractBasePath(), randomName());
        TwoPhaseOutputStream.Committer committer = stageTwoPhaseOutput(target, content);
        committer.commit(contractFileIO());

        committer.clean(contractFileIO());

        assertThat(readBytes(target)).containsExactly(content);
    }

    @Test
    void testTwoPhaseCleanDoesNotAffectAnotherWriter() throws IOException {
        Path target = new Path(contractBasePath(), randomName());
        TwoPhaseOutputStream.Committer first = stageTwoPhaseOutput(target, new byte[] {1, 2, 3});
        byte[] secondContent = new byte[] {4, 5, 6};
        TwoPhaseOutputStream.Committer second = stageTwoPhaseOutput(target, secondContent);

        first.commit(contractFileIO());
        assertThat(contractFileIO().delete(target, false)).isTrue();
        first.clean(contractFileIO());
        second.commit(contractFileIO());

        assertThat(readBytes(target)).containsExactly(secondContent);
    }

    @Test
    void testTwoPhaseCommitterSurvivesSerialization() throws Exception {
        Path target = new Path(contractBasePath(), randomName());
        byte[] content = new byte[] {8, 5, 3, 0, 9};
        TwoPhaseOutputStream.Committer committer = stageTwoPhaseOutput(target, content);

        TwoPhaseOutputStream.Committer restored = InstantiationUtil.clone(committer);

        assertThat(restored.targetPath()).isEqualTo(target);
        restored.commit(contractFileIO());
        assertThat(readBytes(target)).containsExactly(content);

        Path discardedTarget = new Path(contractBasePath(), randomName());
        TwoPhaseOutputStream.Committer discarded =
                InstantiationUtil.clone(
                        stageTwoPhaseOutput(discardedTarget, new byte[] {1, 4, 1, 4}));
        discarded.discard(contractFileIO());
        assertThat(contractFileIO().exists(discardedTarget)).isFalse();

        Path overwrittenTarget = new Path(contractBasePath(), randomName());
        writeBytes(overwrittenTarget, new byte[] {9, 9, 9}, false);
        byte[] replacement = new byte[] {2, 6, 5, 3};
        TwoPhaseOutputStream.Committer overwriting =
                InstantiationUtil.clone(stageTwoPhaseOutput(overwrittenTarget, replacement, true));
        overwriting.commit(contractFileIO());
        assertThat(readBytes(overwrittenTarget)).containsExactly(replacement);
    }

    private FileIO contractFileIO() throws IOException {
        initializeContractFixture();
        return contractFileIO;
    }

    private Path contractBasePath() throws IOException {
        initializeContractFixture();
        return contractBasePath;
    }

    private void initializeContractFixture() throws IOException {
        if (contractFileIO != null) {
            return;
        }

        try {
            FileIO fileIO = getFileSystem();
            Path basePath = new Path(getBasePath(), randomName());
            fileIO.mkdirs(basePath);
            contractFileIO = fileIO;
            contractBasePath = basePath;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void writeBytes(Path file, byte[] content, boolean overwrite) throws IOException {
        try (PositionOutputStream out = contractFileIO().newOutputStream(file, overwrite)) {
            out.write(content);
        }
    }

    private TwoPhaseOutputStream.Committer stageTwoPhaseOutput(Path target, byte[] content)
            throws IOException {
        return stageTwoPhaseOutput(target, content, false);
    }

    private TwoPhaseOutputStream.Committer stageTwoPhaseOutput(
            Path target, byte[] content, boolean overwrite) throws IOException {
        TwoPhaseOutputStream out = contractFileIO().newTwoPhaseOutputStream(target, overwrite);
        out.write(content);
        return out.closeForCommit();
    }

    private byte[] readBytes(Path file) throws IOException {
        try (SeekableInputStream in = contractFileIO().newInputStream(file)) {
            return readAll(in);
        }
    }

    private void assertOpenOrFirstReadFails(Path path) throws IOException {
        final SeekableInputStream in;
        try {
            in = contractFileIO().newInputStream(path);
        } catch (IOException expectedAtOpen) {
            return;
        }

        try {
            assertThatThrownBy(in::read).isInstanceOf(IOException.class);
        } finally {
            try {
                in.close();
            } catch (IOException ignoredAtClose) {
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
        contractFileIO().mkdirs(directory);
        Path file = new Path(directory, randomName());
        writeBytes(file, content, false);
        return file;
    }
}
