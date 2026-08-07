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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileUtils}. */
public class FileUtilsTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testListVersionedFilesIgnoresInvalidVersions() throws IOException {
        FileIO fileIO = LocalFileIO.create();
        Path directory = new Path(tempDir.toString(), "snapshot");
        fileIO.mkdirs(directory);
        fileIO.writeFile(new Path(directory, "snapshot-1"), "", false);
        String uuid = "d686aba1-b44a-40a4-a4f1-d854830aa5cb";
        fileIO.writeFile(new Path(directory, "snapshot-2" + uuid + ".tmp"), "", false);
        fileIO.writeFile(new Path(directory, "snapshot-3." + uuid + ".tmp"), "", false);
        fileIO.writeFile(new Path(directory, "snapshot-999999999999999999999999999"), "", false);
        fileIO.writeFile(new Path(directory, "unrelated"), "", false);

        List<Long> versions =
                FileUtils.listVersionedFiles(fileIO, directory, "snapshot-")
                        .collect(Collectors.toList());

        assertThat(versions).containsExactly(1L);
    }
}
