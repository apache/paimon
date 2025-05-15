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

package org.apache.paimon.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LocalFileUtils}. */
public class LocalFileUtilsTest {

    @TempDir private java.nio.file.Path temporaryFolder;

    @Test
    void testGetTargetPathNotContainsSymbolicPath() throws IOException {
        java.nio.file.Path testPath = Paths.get("parent", "child");
        java.nio.file.Path targetPath =
                LocalFileUtils.getTargetPathIfContainsSymbolicPath(testPath);
        assertThat(targetPath).isEqualTo(testPath);
    }

    @Test
    void testGetTargetPathContainsSymbolicPath() throws IOException {
        File linkedDir = TempDirUtils.newFolder(temporaryFolder, "linked");
        java.nio.file.Path symlink = Paths.get(temporaryFolder.toString(), "symlink");
        java.nio.file.Path dirInLinked =
                TempDirUtils.newFolder(linkedDir.toPath(), "one", "two").toPath().toRealPath();
        Files.createSymbolicLink(symlink, linkedDir.toPath());

        java.nio.file.Path targetPath =
                LocalFileUtils.getTargetPathIfContainsSymbolicPath(
                        symlink.resolve("one").resolve("two"));
        assertThat(targetPath).isEqualTo(dirInLinked);
    }

    @Test
    void testGetTargetPathContainsMultipleSymbolicPath() throws IOException {
        File linked1Dir = TempDirUtils.newFolder(temporaryFolder, "linked1");
        java.nio.file.Path symlink1 = Paths.get(temporaryFolder.toString(), "symlink1");
        Files.createSymbolicLink(symlink1, linked1Dir.toPath());

        java.nio.file.Path symlink2 = Paths.get(symlink1.toString(), "symlink2");
        File linked2Dir = TempDirUtils.newFolder(temporaryFolder, "linked2");
        Files.createSymbolicLink(symlink2, linked2Dir.toPath());
        java.nio.file.Path dirInLinked2 =
                TempDirUtils.newFolder(linked2Dir.toPath(), "one").toPath().toRealPath();

        // symlink3 point to another symbolic link: symlink2
        java.nio.file.Path symlink3 = Paths.get(symlink1.toString(), "symlink3");
        Files.createSymbolicLink(symlink3, symlink2);

        java.nio.file.Path targetPath =
                LocalFileUtils.getTargetPathIfContainsSymbolicPath(
                        // path contains multiple symlink : xxx/symlink1/symlink3/one
                        symlink3.resolve("one"));
        assertThat(targetPath).isEqualTo(dirInLinked2);
    }
}
