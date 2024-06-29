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

package org.apache.paimon.fs.local;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LocalFileIO}. */
public class LocalFleIOTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testCopy() throws Exception {
        Path srcFile = new Path(tempDir.resolve("src.txt").toUri());
        Path dstFile = new Path(tempDir.resolve("dst.txt").toUri());

        FileIO fileIO = new LocalFileIO();
        fileIO.writeFileUtf8(srcFile, "foobar");

        fileIO.copyFile(srcFile, dstFile, false);
        assertThat(fileIO.readFileUtf8(dstFile)).isEqualTo("foobar");
        fileIO.deleteQuietly(dstFile);

        fileIO.copyFile(srcFile, dstFile, false);
        assertThat(fileIO.readFileUtf8(dstFile)).isEqualTo("foobar");
    }
}
