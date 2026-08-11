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

import org.apache.paimon.fs.hadoop.HadoopFileIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

/** Behavior tests for Hadoop Local. */
class HadoopLocalFileIOBehaviorTest extends FileIOBehaviorTestBase {

    @TempDir private java.nio.file.Path tmp;

    @Override
    protected FileIO getFileSystem() throws Exception {
        org.apache.hadoop.fs.FileSystem fs = new RawLocalFileSystem();
        fs.initialize(URI.create("file:///"), new Configuration());
        HadoopFileIO fileIO = new HadoopFileIO(getBasePath());
        fileIO.setFileSystem(fs);
        return fileIO;
    }

    @Override
    protected Path getBasePath() {
        return new Path(tmp.toUri());
    }

    @Test
    void testIsObjectStoreReturnsFalse() throws Exception {
        assertThat(getFileSystem().isObjectStore()).isFalse();
    }
}
