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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Common tests for the behavior of {@link FileIO} methods. */
public abstract class FileIOBehaviorTestBase {

    private static final Random RND = new Random();

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

    // ------------------------------------------------------------------------
    //  Suite of Tests
    // ------------------------------------------------------------------------

    // --- exists

    @Test
    void testFileExists() throws IOException {
        final Path filePath = createRandomFileInDirectory(basePath);
        assertThat(fs.exists(filePath)).isTrue();
    }

    @Test
    void testFileDoesNotExist() throws IOException {
        assertThat(fs.exists(new Path(basePath, randomName()))).isFalse();
    }

    // --- list files

    @Test
    void testListFilesIterativeNonRecursive() throws IOException {
        Path fileA = createRandomFileInDirectory(basePath);
        Path dirB = new Path(basePath, randomName());
        fs.mkdirs(dirB);
        Path fileBC = createRandomFileInDirectory(dirB);

        List<FileStatus> allFiles = new ArrayList<>();
        try (RemoteIterator<FileStatus> iter = fs.listFilesIterative(basePath, false)) {
            while (iter.hasNext()) {
                allFiles.add(iter.next());
            }
        }
        assertThat(allFiles.size()).isEqualTo(1);
        assertThat(allFiles.get(0).getPath()).isEqualTo(fileA);
    }

    @Test
    void testListFilesIterativeRecursive() throws IOException {
        Path fileA = createRandomFileInDirectory(basePath);
        Path dirB = new Path(basePath, randomName());
        fs.mkdirs(dirB);
        Path fileBC = createRandomFileInDirectory(dirB);

        List<FileStatus> allFiles = new ArrayList<>();
        try (RemoteIterator<FileStatus> iter = fs.listFilesIterative(basePath, true)) {
            while (iter.hasNext()) {
                allFiles.add(iter.next());
            }
        }
        assertThat(allFiles.size()).isEqualTo(2);
        assertThat(allFiles.stream().filter(f -> f.getPath().equals(fileA)).count()).isEqualTo(1);
        assertThat(allFiles.stream().filter(f -> f.getPath().equals(fileBC)).count()).isEqualTo(1);
    }

    // --- delete

    @Test
    void testExistingFileDeletion() throws IOException {
        testSuccessfulDeletion(createRandomFileInDirectory(basePath), false);
    }

    @Test
    void testExistingFileRecursiveDeletion() throws IOException {
        testSuccessfulDeletion(createRandomFileInDirectory(basePath), true);
    }

    @Test
    void testNotExistingFileDeletion() throws IOException {
        testSuccessfulDeletion(new Path(basePath, randomName()), false);
    }

    @Test
    void testNotExistingFileRecursiveDeletion() throws IOException {
        testSuccessfulDeletion(new Path(basePath, randomName()), true);
    }

    @Test
    void testExistingEmptyDirectoryDeletion() throws IOException {
        final Path path = new Path(basePath, randomName());
        fs.mkdirs(path);
        testSuccessfulDeletion(path, false);
    }

    @Test
    void testExistingEmptyDirectoryRecursiveDeletion() throws IOException {
        final Path path = new Path(basePath, randomName());
        fs.mkdirs(path);
        testSuccessfulDeletion(path, true);
    }

    private void testSuccessfulDeletion(Path path, boolean recursionEnabled) throws IOException {
        fs.delete(path, recursionEnabled);
        assertThat(fs.exists(path)).isFalse();
    }

    @Test
    void testExistingNonEmptyDirectoryDeletion() throws IOException {
        final Path directoryPath = new Path(basePath, randomName());
        final Path filePath = createRandomFileInDirectory(directoryPath);

        assertThatThrownBy(() -> fs.delete(directoryPath, false)).isInstanceOf(IOException.class);
        assertThat(fs.exists(directoryPath)).isTrue();
        assertThat(fs.exists(filePath)).isTrue();
    }

    @Test
    void testExistingNonEmptyDirectoryRecursiveDeletion() throws IOException {
        final Path directoryPath = new Path(basePath, randomName());
        final Path filePath = createRandomFileInDirectory(directoryPath);

        fs.delete(directoryPath, true);
        assertThat(fs.exists(directoryPath)).isFalse();
        assertThat(fs.exists(filePath)).isFalse();
    }

    @Test
    void testExistingNonEmptyDirectoryWithSubDirRecursiveDeletion() throws IOException {
        final Path level1SubDirWithFile = new Path(basePath, randomName());
        final Path fileInLevel1Subdir = createRandomFileInDirectory(level1SubDirWithFile);
        final Path level2SubDirWithFile = new Path(level1SubDirWithFile, randomName());
        final Path fileInLevel2Subdir = createRandomFileInDirectory(level2SubDirWithFile);

        testSuccessfulDeletion(level1SubDirWithFile, true);
        assertThat(fs.exists(fileInLevel1Subdir)).isFalse();
        assertThat(fs.exists(level2SubDirWithFile)).isFalse();
        assertThat(fs.exists(fileInLevel2Subdir)).isFalse();
    }

    // --- mkdirs

    @Test
    void testMkdirsReturnsTrueWhenCreatingDirectory() throws Exception {
        // this test applies to object stores as well, as rely on the fact that they
        // return true when things are not bad

        final Path directory = new Path(basePath, randomName());
        assertThat(fs.mkdirs(directory)).isTrue();
        assertThat(fs.exists(directory)).isTrue();
    }

    @Test
    void testMkdirsCreatesParentDirectories() throws Exception {
        // this test applies to object stores as well, as rely on the fact that they
        // return true when things are not bad

        final Path directory =
                new Path(new Path(new Path(basePath, randomName()), randomName()), randomName());
        assertThat(fs.mkdirs(directory)).isTrue();

        assertThat(fs.exists(directory)).isTrue();
    }

    @Test
    void testMkdirsReturnsTrueForExistingDirectory() throws Exception {
        // this test applies to object stores as well, as rely on the fact that they
        // return true when things are not bad

        final Path directory = new Path(basePath, randomName());

        // make sure the directory exists
        createRandomFileInDirectory(directory);

        assertThat(fs.mkdirs(directory)).isTrue();
    }

    @Test
    protected void testMkdirsFailsForExistingFile() throws Exception {
        final Path file = new Path(getBasePath(), randomName());
        createFile(file);

        try {
            fs.mkdirs(file);
            fail("should fail with an IOException");
        } catch (IOException e) {
            // good!
        }
    }

    @Test
    void testMkdirsFailsWithExistingParentFile() throws Exception {
        final Path file = new Path(getBasePath(), randomName());
        createFile(file);

        final Path dirUnderFile = new Path(file, randomName());
        try {
            fs.mkdirs(dirUnderFile);
            fail("should fail with an IOException");
        } catch (IOException e) {
            // good!
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    protected static String randomName() {
        return StringUtils.getRandomString(RND, 16, 16, 'a', 'z');
    }

    private void createFile(Path file) throws IOException {
        try (PositionOutputStream out = fs.newOutputStream(file, false)) {
            out.write(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        }
    }

    private Path createRandomFileInDirectory(Path directory) throws IOException {
        fs.mkdirs(directory);
        final Path filePath = new Path(directory, randomName());
        createFile(filePath);

        return filePath;
    }
}
