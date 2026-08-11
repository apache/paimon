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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.fs.RecordingFileIO.Method.DELETE;
import static org.apache.paimon.fs.RecordingFileIO.Method.EXISTS;
import static org.apache.paimon.fs.RecordingFileIO.Method.GET_FILE_STATUS;
import static org.apache.paimon.fs.RecordingFileIO.Method.INPUT_READ;
import static org.apache.paimon.fs.RecordingFileIO.Method.LIST_STATUS;
import static org.apache.paimon.fs.RecordingFileIO.Method.MKDIRS;
import static org.apache.paimon.fs.RecordingFileIO.Method.NEW_INPUT_STREAM;
import static org.apache.paimon.fs.RecordingFileIO.Method.NEW_OUTPUT_STREAM;
import static org.apache.paimon.fs.RecordingFileIO.Method.OUTPUT_WRITE;
import static org.apache.paimon.fs.RecordingFileIO.Method.RENAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Contract tests for methods implemented directly on {@link FileIO}. */
class FileIODefaultMethodTest {

    private RecordingFileIO fileIO;
    private Path root;

    @BeforeEach
    void beforeEach() {
        fileIO = new RecordingFileIO();
        root = new Path("test:///root");
        fileIO.putDirectory(root);
        fileIO.reset();
    }

    @Test
    void lifecycleDefaultsAreNoOps() throws Exception {
        fileIO.setRuntimeContext(Collections.singletonMap("key", "value"));
        fileIO.close();

        assertThat(fileIO.calls()).isEmpty();
    }

    @Test
    void statusHelpersUseOnlyFileStatus() throws Exception {
        Path file = new Path(root, "file");
        Path directory = new Path(root, "directory");
        fileIO.putFile(file, "你好");
        fileIO.putDirectory(directory);
        fileIO.reset();

        assertThat(fileIO.getFileSize(file))
                .isEqualTo("你好".getBytes(StandardCharsets.UTF_8).length);
        assertThat(fileIO.calls()).containsExactly(RecordingFileIO.call(GET_FILE_STATUS, file));

        fileIO.reset();
        assertThat(fileIO.isDir(file)).isFalse();
        assertThat(fileIO.calls()).containsExactly(RecordingFileIO.call(GET_FILE_STATUS, file));

        fileIO.reset();
        assertThat(fileIO.isDir(directory)).isTrue();
        assertThat(fileIO.calls())
                .containsExactly(RecordingFileIO.call(GET_FILE_STATUS, directory));

        fileIO.reset();
        fileIO.failNext(GET_FILE_STATUS, new IOException("status failed"));
        assertThatThrownBy(() -> fileIO.getFileSize(file))
                .isInstanceOf(IOException.class)
                .hasMessage("status failed");

        fileIO.reset();
        fileIO.failNext(GET_FILE_STATUS, new IOException("type failed"));
        assertThatThrownBy(() -> fileIO.isDir(file))
                .isInstanceOf(IOException.class)
                .hasMessage("type failed");
    }

    @Test
    void checkOrMkdirsAvoidsUnneededMutations() throws Exception {
        Path existingDirectory = new Path(root, "directory");
        fileIO.putDirectory(existingDirectory);
        fileIO.reset();

        fileIO.checkOrMkdirs(existingDirectory);

        assertThat(fileIO.callCount(EXISTS) + fileIO.callCount(GET_FILE_STATUS)).isBetween(1L, 2L);
        assertThat(fileIO.callCount(MKDIRS)).isZero();
        assertOnlyCalls(EXISTS, GET_FILE_STATUS);

        Path missing = new Path(root, "created");
        fileIO.reset();
        fileIO.checkOrMkdirs(missing);
        assertThat(fileIO.isDirectoryInMemory(missing)).isTrue();
        assertThat(fileIO.callCount(EXISTS) + fileIO.callCount(GET_FILE_STATUS))
                .isLessThanOrEqualTo(1L);
        assertThat(fileIO.callCount(MKDIRS)).isEqualTo(1);
        assertOnlyCalls(EXISTS, GET_FILE_STATUS, MKDIRS);

        Path file = new Path(root, "file");
        fileIO.putFile(file, "content");
        fileIO.reset();
        assertThatThrownBy(() -> fileIO.checkOrMkdirs(file))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("should be a directory");
        assertThat(fileIO.callCount(MKDIRS)).isZero();
        assertOnlyCalls(EXISTS, GET_FILE_STATUS);
    }

    @Test
    void quietDeletesUseTheRequestedRecursionAndSuppressIoFailures() {
        Path file = new Path(root, "file");
        fileIO.putFile(file, "content");
        fileIO.deleteQuietly(file);
        assertThat(fileIO.existsInMemory(file)).isFalse();
        assertThat(fileIO.calls(DELETE)).containsExactly(RecordingFileIO.call(DELETE, file, false));
        assertThat(fileIO.callCount(EXISTS)).isZero();

        Path first = new Path(root, "first");
        Path second = new Path(root, "second");
        fileIO.putFile(first, "1");
        fileIO.putFile(second, "2");
        fileIO.reset();
        fileIO.deleteFilesQuietly(Arrays.asList(first, second));
        assertThat(fileIO.existsInMemory(first)).isFalse();
        assertThat(fileIO.existsInMemory(second)).isFalse();
        assertThat(fileIO.calls())
                .containsExactly(
                        RecordingFileIO.call(DELETE, first, false),
                        RecordingFileIO.call(DELETE, second, false));

        fileIO.putFile(first, "1");
        fileIO.putFile(second, "2");
        fileIO.reset();
        fileIO.failNext(DELETE, new IOException("first failed"));
        fileIO.deleteFilesQuietly(Arrays.asList(first, second));
        assertThat(fileIO.existsInMemory(first)).isTrue();
        assertThat(fileIO.existsInMemory(second)).isFalse();
        assertThat(fileIO.calls(DELETE))
                .containsExactly(
                        RecordingFileIO.call(DELETE, first, false),
                        RecordingFileIO.call(DELETE, second, false));

        Path directory = new Path(root, "directory");
        fileIO.putDirectory(directory);
        fileIO.reset();
        fileIO.deleteDirectoryQuietly(directory);
        assertThat(fileIO.calls(DELETE))
                .containsExactly(RecordingFileIO.call(DELETE, directory, true));

        Path missing = new Path(root, "missing");
        fileIO.reset();
        fileIO.deleteQuietly(missing);
        assertThat(fileIO.callCount(DELETE)).isEqualTo(1);
        assertThat(fileIO.callCount(EXISTS)).isLessThanOrEqualTo(1);

        fileIO.putFile(file, "content");
        fileIO.reset();
        fileIO.failNext(DELETE, new IOException("planned"));
        fileIO.deleteQuietly(file);
        assertThat(fileIO.existsInMemory(file)).isTrue();
        assertThat(fileIO.callCount(DELETE)).isEqualTo(1);
        assertThat(fileIO.callCount(EXISTS)).isZero();
    }

    @Test
    void utf8HelpersPreserveContentAndForwardOverwriteMode() throws Exception {
        Path file = new Path(root, "unicode");
        String content = "Paimon-文件-🙂";

        fileIO.writeFile(file, content, false);
        assertThat(fileIO.fileContent(file)).isEqualTo(content);
        assertThat(fileIO.openOutputStreams()).isZero();
        assertThat(fileIO.calls(NEW_OUTPUT_STREAM))
                .containsExactly(RecordingFileIO.call(NEW_OUTPUT_STREAM, file, false));

        fileIO.reset();
        assertThat(fileIO.readFileUtf8(file)).isEqualTo(content);
        assertThat(fileIO.openInputStreams()).isZero();
        assertThat(fileIO.calls(NEW_INPUT_STREAM))
                .containsExactly(RecordingFileIO.call(NEW_INPUT_STREAM, file));

        fileIO.reset();
        fileIO.overwriteFileUtf8(file, "replacement");
        assertThat(fileIO.fileContent(file)).isEqualTo("replacement");
        assertThat(fileIO.calls(NEW_OUTPUT_STREAM))
                .containsExactly(RecordingFileIO.call(NEW_OUTPUT_STREAM, file, true));

        fileIO.reset();
        fileIO.overwriteHintFile(file, "hint");
        assertThat(fileIO.fileContent(file)).isEqualTo("hint");
        assertThat(fileIO.calls(NEW_OUTPUT_STREAM))
                .containsExactly(RecordingFileIO.call(NEW_OUTPUT_STREAM, file, true));
    }

    @Test
    void listingDefaultsReturnFilesOrDirectoriesWithoutEagerRelisting() throws Exception {
        Path topFile = new Path(root, "top");
        Path directory = new Path(root, "directory");
        Path nestedFile = new Path(directory, "nested");
        fileIO.putFile(topFile, "top");
        fileIO.putDirectory(directory);
        fileIO.putFile(nestedFile, "nested");
        fileIO.reset();

        assertThat(fileIO.listFiles(root, false))
                .extracting(FileStatus::getPath)
                .containsExactlyInAnyOrder(topFile);
        assertThat(fileIO.callCount(LIST_STATUS)).isEqualTo(1);
        assertNoMetadataPreflight();

        fileIO.reset();
        assertThat(fileIO.listFiles(root, true))
                .extracting(FileStatus::getPath)
                .containsExactlyInAnyOrder(topFile, nestedFile);
        assertThat(fileIO.callCount(LIST_STATUS)).isLessThanOrEqualTo(2);
        assertNoMetadataPreflight();

        fileIO.reset();
        RemoteIterator<FileStatus> iterator = fileIO.listFilesIterative(root, true);
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.hasNext()).isTrue();
        assertThat(collect(iterator))
                .extracting(FileStatus::getPath)
                .containsExactlyInAnyOrder(topFile, nestedFile);
        assertThat(fileIO.callCount(LIST_STATUS)).isLessThanOrEqualTo(2);
        assertNoMetadataPreflight();

        fileIO.reset();
        assertThat(fileIO.listDirectories(root))
                .extracting(FileStatus::getPath)
                .containsExactlyInAnyOrder(directory);
        assertThat(fileIO.callCount(LIST_STATUS)).isEqualTo(1);
        assertNoMetadataPreflight();

        Path empty = new Path(root, "empty");
        fileIO.putDirectory(empty);
        fileIO.reset();
        assertThat(fileIO.listFiles(empty, true)).isEmpty();
        assertThat(fileIO.listDirectories(empty)).isEmpty();

        fileIO.reset();
        fileIO.failNext(LIST_STATUS, new IOException("lazy listing failed"));
        RemoteIterator<FileStatus> failing = fileIO.listFilesIterative(root, true);
        assertThatThrownBy(failing::hasNext)
                .isInstanceOf(IOException.class)
                .hasMessage("lazy listing failed");
    }

    @Test
    void copyDefaultsTransferBytesAndForwardOverwriteMode() throws Exception {
        Path source = new Path(root, "source");
        Path target = new Path(root, "target");
        fileIO.putFile(source, "source-文件");
        fileIO.reset();

        fileIO.copyFile(source, target, false);

        assertThat(fileIO.fileContent(target)).isEqualTo("source-文件");
        assertThat(fileIO.fileContent(source)).isEqualTo("source-文件");
        assertThat(fileIO.openInputStreams()).isZero();
        assertThat(fileIO.openOutputStreams()).isZero();
        assertThat(fileIO.calls(NEW_INPUT_STREAM))
                .containsExactly(RecordingFileIO.call(NEW_INPUT_STREAM, source));
        assertThat(fileIO.calls(NEW_OUTPUT_STREAM))
                .containsExactly(RecordingFileIO.call(NEW_OUTPUT_STREAM, target, false));
        assertNoCopyPreflights(0);

        Path sourceDirectory = new Path(root, "sources");
        Path targetDirectory = new Path(root, "targets");
        Path first = new Path(sourceDirectory, "first");
        Path second = new Path(sourceDirectory, "second");
        fileIO.putDirectory(sourceDirectory);
        fileIO.putDirectory(targetDirectory);
        fileIO.putFile(first, "1");
        fileIO.putFile(second, "2");
        fileIO.reset();

        fileIO.copyFiles(sourceDirectory, targetDirectory, true);

        assertThat(fileIO.fileContent(new Path(targetDirectory, "first"))).isEqualTo("1");
        assertThat(fileIO.fileContent(new Path(targetDirectory, "second"))).isEqualTo("2");
        assertThat(fileIO.fileContent(first)).isEqualTo("1");
        assertThat(fileIO.fileContent(second)).isEqualTo("2");
        assertThat(fileIO.callCount(LIST_STATUS)).isEqualTo(1);
        assertThat(fileIO.callCount(NEW_INPUT_STREAM)).isEqualTo(2);
        assertThat(fileIO.calls(NEW_OUTPUT_STREAM))
                .containsExactlyInAnyOrder(
                        RecordingFileIO.call(
                                NEW_OUTPUT_STREAM, new Path(targetDirectory, "first"), true),
                        RecordingFileIO.call(
                                NEW_OUTPUT_STREAM, new Path(targetDirectory, "second"), true));
        assertNoCopyPreflights(1);

        Path failedTarget = new Path(root, "failed-target");
        fileIO.reset();
        fileIO.failNext(NEW_OUTPUT_STREAM, new IOException("target open failed"));
        assertThatThrownBy(() -> fileIO.copyFile(source, failedTarget, false))
                .isInstanceOf(IOException.class)
                .hasMessage("target open failed");
        assertThat(fileIO.openInputStreams()).isZero();
        assertThat(fileIO.openOutputStreams()).isZero();
        assertThat(fileIO.existsInMemory(failedTarget)).isFalse();
        assertThat(fileIO.existsInMemory(source)).isTrue();
        assertNoCopyPreflights(0);

        fileIO.reset();
        fileIO.failNext(INPUT_READ, new IOException("source read failed"));
        assertThatThrownBy(() -> fileIO.copyFile(source, new Path(root, "read-failed"), false))
                .isInstanceOf(IOException.class)
                .hasMessage("source read failed");
        assertThat(fileIO.openInputStreams()).isZero();
        assertThat(fileIO.openOutputStreams()).isZero();
        assertNoCopyPreflights(0);

        fileIO.reset();
        fileIO.failNext(OUTPUT_WRITE, new IOException("target write failed"));
        assertThatThrownBy(() -> fileIO.copyFile(source, new Path(root, "write-failed"), false))
                .isInstanceOf(IOException.class)
                .hasMessage("target write failed");
        assertThat(fileIO.openInputStreams()).isZero();
        assertThat(fileIO.openOutputStreams()).isZero();
        assertNoCopyPreflights(0);
    }

    @Test
    void tryToWriteAtomicPublishesOrCleansUpTheTemporaryFile() throws Exception {
        Path target = new Path(root, "atomic");

        assertThat(fileIO.tryToWriteAtomic(target, "new")).isTrue();
        assertThat(fileIO.fileContent(target)).isEqualTo("new");
        assertThat(fileIO.callCount(NEW_OUTPUT_STREAM)).isEqualTo(1);
        assertThat(fileIO.callCount(RENAME)).isEqualTo(1);
        assertThat(fileIO.callCount(DELETE)).isZero();
        assertThat(fileIO.callCount(EXISTS)).isZero();
        assertThat(fileIO.callCount(GET_FILE_STATUS)).isZero();
        assertThat(fileIO.callCount(LIST_STATUS)).isZero();
        assertAtomicTemporaryPath(target, fileIO.calls(RENAME).get(0));

        Path failedTarget = new Path(root, "failed-atomic");
        fileIO.reset();
        fileIO.failNext(RENAME, new IOException("rename failed"));
        assertThatThrownBy(() -> fileIO.tryToWriteAtomic(failedTarget, "content"))
                .isInstanceOf(IOException.class)
                .hasMessage("rename failed");
        RecordingFileIO.Call failedRename = fileIO.calls(RENAME).get(0);
        Path failedTemporary = failedRename.argument(0, Path.class);
        assertThat(fileIO.existsInMemory(failedTemporary)).isFalse();
        assertThat(fileIO.existsInMemory(failedTarget)).isFalse();
        assertThat(fileIO.callCount(DELETE)).isEqualTo(1);
        assertThat(fileIO.callCount(LIST_STATUS)).isZero();

        fileIO.putFile(target, "existing");
        fileIO.reset();
        assertThat(fileIO.tryToWriteAtomic(target, "replacement")).isFalse();
        assertThat(fileIO.fileContent(target)).isEqualTo("existing");
        assertThat(fileIO.callCount(NEW_OUTPUT_STREAM)).isEqualTo(1);
        assertThat(fileIO.callCount(RENAME)).isEqualTo(1);
        assertThat(fileIO.callCount(DELETE)).isEqualTo(1);
        assertThat(fileIO.callCount(EXISTS)).isZero();
        assertThat(fileIO.callCount(LIST_STATUS)).isZero();
        assertAtomicTemporaryPath(target, fileIO.calls(RENAME).get(0));
    }

    @Test
    void defaultTwoPhaseOutputStagesThenPublishesOnCommit() throws Exception {
        Path target = new Path(root, "two-phase");
        String content = "staged-文件";

        TwoPhaseOutputStream stream = fileIO.newTwoPhaseOutputStream(target, false);
        stream.write(content.getBytes(StandardCharsets.UTF_8));
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        assertThat(fileIO.existsInMemory(target)).isFalse();
        assertThat(committer.targetPath()).isEqualTo(target);
        committer.commit(fileIO);
        assertThat(fileIO.fileContent(target)).isEqualTo(content);
        assertThat(fileIO.openOutputStreams()).isZero();
    }

    @Test
    void overwrittenReadReturnsEmptyForMissingFilesWithoutExistenceProbe() throws Exception {
        Path missing = new Path(root, "missing");

        assertThat(fileIO.readOverwrittenFileUtf8(missing)).isEmpty();
        assertThat(fileIO.callCount(NEW_INPUT_STREAM)).isEqualTo(1);
        assertThat(fileIO.callCount(EXISTS)).isZero();

        Path disappeared = new Path(root, "disappeared");
        fileIO.reset();
        fileIO.failNext(NEW_INPUT_STREAM, new IOException("transient"));
        assertThat(fileIO.readOverwrittenFileUtf8(disappeared)).isEmpty();
        assertThat(fileIO.callCount(EXISTS)).isLessThanOrEqualTo(1);
    }

    @Test
    void overwrittenReadRetriesOnlyKnownConcurrentChangeFailures() throws Exception {
        Path file = new Path(root, "overwritten");
        fileIO.putFile(file, "stable");
        fileIO.failNext(NEW_INPUT_STREAM, blocklistChanged());
        fileIO.failNext(NEW_INPUT_STREAM, blocklistChanged());

        assertThat(fileIO.readOverwrittenFileUtf8(file)).contains("stable");
        assertThat(fileIO.callCount(NEW_INPUT_STREAM)).isEqualTo(3);
        assertThat(fileIO.callCount(EXISTS)).isLessThanOrEqualTo(2);

        fileIO.reset();
        fileIO.failNext(NEW_INPUT_STREAM, new IOException("unrelated"));
        assertThatThrownBy(() -> fileIO.readOverwrittenFileUtf8(file))
                .isInstanceOf(IOException.class)
                .hasMessage("unrelated");
        assertThat(fileIO.callCount(NEW_INPUT_STREAM)).isEqualTo(1);
        assertThat(fileIO.callCount(EXISTS)).isLessThanOrEqualTo(1);
    }

    @Test
    void overwrittenReadStopsAfterFiveKnownFailures() {
        Path file = new Path(root, "overwritten");
        fileIO.putFile(file, "stable");
        for (int i = 0; i < 5; i++) {
            fileIO.failNext(NEW_INPUT_STREAM, blocklistChanged());
        }

        assertThatThrownBy(() -> fileIO.readOverwrittenFileUtf8(file))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Blocklist for");
        assertThat(fileIO.callCount(NEW_INPUT_STREAM)).isEqualTo(5);
        assertThat(fileIO.callCount(EXISTS)).isLessThanOrEqualTo(5);
    }

    private static IOException blocklistChanged() {
        return new IOException("Blocklist for test has changed");
    }

    private void assertNoMetadataPreflight() {
        assertThat(fileIO.callCount(GET_FILE_STATUS)).isZero();
        assertThat(fileIO.callCount(EXISTS)).isZero();
    }

    private void assertNoCopyPreflights(long expectedListCalls) {
        assertNoMetadataPreflight();
        assertThat(fileIO.callCount(DELETE)).isZero();
        assertThat(fileIO.callCount(LIST_STATUS)).isEqualTo(expectedListCalls);
    }

    private void assertOnlyCalls(RecordingFileIO.Method... allowedMethods) {
        List<RecordingFileIO.Method> allowed = Arrays.asList(allowedMethods);
        assertThat(fileIO.calls()).allMatch(call -> allowed.contains(call.method()));
    }

    private static List<FileStatus> collect(RemoteIterator<FileStatus> iterator)
            throws IOException {
        List<FileStatus> result = new ArrayList<>();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        return result;
    }

    private static void assertAtomicTemporaryPath(Path target, RecordingFileIO.Call renameCall) {
        Path temporary = renameCall.argument(0, Path.class);
        assertThat(renameCall.argument(1, Path.class)).isEqualTo(target);
        assertThat(temporary.getParent()).isEqualTo(target.getParent());
        assertThat(temporary.getName()).startsWith("." + target.getName() + ".").endsWith(".tmp");
    }
}
