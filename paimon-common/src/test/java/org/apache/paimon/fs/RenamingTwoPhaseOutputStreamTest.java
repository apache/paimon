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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RenamingTwoPhaseOutputStream}. */
public class RenamingTwoPhaseOutputStreamTest {

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private Path targetPath;

    @BeforeEach
    void setup() {
        fileIO = new LocalFileIO();
        targetPath = new Path(tempDir.resolve("target-file.txt").toString());
    }

    @Test
    void testSuccessfulCommit() throws IOException {
        RenamingTwoPhaseOutputStream stream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);

        // Write some data
        String testData = "Hello, World!";
        stream.write(testData.getBytes());

        // Close for commit
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        // Target file should not exist yet
        assertThat(fileIO.exists(targetPath)).isFalse();

        // Commit the file
        committer.commit(fileIO);

        // Now target file should exist with correct content
        assertThat(fileIO.exists(targetPath)).isTrue();

        // Read and verify content
        byte[] content = Files.readAllBytes(Paths.get(targetPath.toString()));
        assertThat(new String(content)).isEqualTo(testData);
    }

    @Test
    void testCleanKeepsAStagingDirectoryHoldingAnotherWritersFile() throws IOException {
        RenamingTwoPhaseOutputStream stream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);
        stream.write("Some data".getBytes());
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        // A MapReduce-style writer with a task attempt still pending in the same directory.
        Path otherWriterPending =
                new Path(targetPath.getParent(), "_temporary/attempt_0001_m_000010_15/part-00010");
        fileIO.writeFile(otherWriterPending, "concurrent", false);

        committer.commit(fileIO);
        committer.clean(fileIO);

        assertThat(fileIO.exists(targetPath)).isTrue();
        assertThat(fileIO.exists(otherWriterPending)).isTrue();
        assertThat(fileIO.exists(new Path(targetPath.getParent(), "_temporary"))).isTrue();
    }

    @Test
    void testCleanKeepsAStagingDirectoryThatIsEmpty() throws IOException {
        RenamingTwoPhaseOutputStream stream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);
        stream.write("Some data".getBytes());
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();
        Path stagingDir = new Path(targetPath.getParent(), "_temporary");

        committer.commit(fileIO);
        committer.clean(fileIO);

        // Empty is not the same as unused: a writer that has just created '_temporary' has not
        // staged its file in it yet, and removing the directory would fail that writer's open.
        // exists(), not listStatus(): listStatus answers with no entries for a directory that is
        // gone as much as for one that is empty, so it cannot tell the two apart.
        assertThat(fileIO.exists(targetPath)).isTrue();
        assertThat(fileIO.exists(stagingDir)).isTrue();
        assertThat(fileIO.listStatus(stagingDir)).isEmpty();
    }

    @Test
    void testCleanRemovesTheFileItStagedWhenThereWasNoCommit() throws IOException {
        RenamingTwoPhaseOutputStream stream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);
        stream.write("Some data".getBytes());
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        // No commit renamed it away, so clean() is what keeps the staged file from being left
        // behind for good.
        committer.clean(fileIO);

        Path stagingDir = new Path(targetPath.getParent(), "_temporary");
        assertThat(fileIO.exists(targetPath)).isFalse();
        assertThat(fileIO.exists(stagingDir)).isTrue();
        assertThat(fileIO.listStatus(stagingDir)).isEmpty();
    }

    @Test
    void testDiscard() throws IOException {
        RenamingTwoPhaseOutputStream stream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);

        // Write some data
        stream.write("Some data".getBytes());

        // Close for commit
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        // Discard instead of commit
        committer.discard(fileIO);

        // Target file should not exist
        assertThat(fileIO.exists(targetPath)).isFalse();
    }

    @Test
    void testDiscardRemovesOnlyItsStagedFile() throws IOException {
        RenamingTwoPhaseOutputStream stream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);
        stream.write("abandoned".getBytes());
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        Path stagingDir = new Path(targetPath.getParent(), "_temporary");
        FileStatus[] stagedFiles = fileIO.listStatus(stagingDir);
        assertThat(stagedFiles).hasSize(1);
        Path stagedPath = stagedFiles[0].getPath();

        Path otherWriterPending = new Path(stagingDir, "attempt_0001_m_000010_15/part-00010");
        fileIO.writeFile(otherWriterPending, "concurrent", false);
        fileIO.writeFile(targetPath, "published", false);

        committer.discard(fileIO);

        assertThat(fileIO.exists(stagedPath)).isFalse();
        assertThat(fileIO.exists(otherWriterPending)).isTrue();
        assertThat(fileIO.readFileUtf8(targetPath)).isEqualTo("published");
    }

    @Test
    void testOverwriteDoesNotDeleteTargetWhenStagedFileIsMissing() throws IOException {
        fileIO.writeFile(targetPath, "old", false);
        RenamingTwoPhaseOutputStream stream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, true);
        stream.write("new".getBytes());
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        Path stagingDir = new Path(targetPath.getParent(), "_temporary");
        FileStatus[] stagedFiles = fileIO.listStatus(stagingDir);
        assertThat(stagedFiles).hasSize(1);
        fileIO.delete(stagedFiles[0].getPath(), false);

        assertThatThrownBy(() -> committer.commit(fileIO)).isInstanceOf(IOException.class);
        assertThat(fileIO.readFileUtf8(targetPath)).isEqualTo("old");
    }

    @Test
    void testCloseWithoutCommit() throws IOException {
        RenamingTwoPhaseOutputStream stream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);
        // Write some data
        stream.write("Some data".getBytes());

        // Just close (not closeForCommit)
        stream.close();

        // Target file should not exist (temp file cleaned up)
        assertThat(fileIO.exists(targetPath)).isFalse();
    }

    @Test
    void testDoubleCommitThrows() throws IOException {
        RenamingTwoPhaseOutputStream stream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);

        stream.write("data".getBytes());
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        // First commit should succeed
        committer.commit(fileIO);

        // Second commit should throw
        assertThatThrownBy(() -> committer.commit(fileIO)).isInstanceOf(IOException.class);
    }

    @Test
    void testPositionTracking() throws IOException {
        RenamingTwoPhaseOutputStream stream =
                new RenamingTwoPhaseOutputStream(fileIO, targetPath, false);

        assertThat(stream.getPos()).isEqualTo(0);

        stream.write("Hello".getBytes());
        assertThat(stream.getPos()).isEqualTo(5);

        stream.write(" World!".getBytes());
        assertThat(stream.getPos()).isEqualTo(12);

        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();
        committer.commit(fileIO);

        // Verify final content
        byte[] content = Files.readAllBytes(Paths.get(targetPath.toString()));
        assertThat(new String(content)).isEqualTo("Hello World!");
    }
}
