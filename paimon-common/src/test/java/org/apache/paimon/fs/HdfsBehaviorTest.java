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
import org.apache.paimon.utils.OperatingSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Behavior tests for HDFS. */
class HdfsBehaviorTest extends FileIOBehaviorTestBase {

    private static MiniDFSCluster hdfsCluster;

    private static HadoopFileIO fs;

    private static Path basePath;

    // ------------------------------------------------------------------------

    @BeforeAll
    static void verifyOS() {
        assumeThat(OperatingSystem.isWindows())
                .describedAs("HDFS cluster cannot be started on Windows without extensions.")
                .isFalse();
    }

    @BeforeAll
    static void createHDFS(@TempDir File tmp) throws Exception {
        Configuration hdConf = new Configuration();
        hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmp.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
        hdfsCluster = builder.build();

        org.apache.hadoop.fs.FileSystem hdfs = hdfsCluster.getFileSystem();

        basePath = new Path(hdfs.getUri().toString() + "/tests");
        fs = new HadoopFileIO();
        fs.setFileSystem(basePath, hdfs);
    }

    @AfterAll
    static void destroyHDFS() throws Exception {
        if (hdfsCluster != null) {
            hdfsCluster
                    .getFileSystem()
                    .delete(new org.apache.hadoop.fs.Path(basePath.toUri()), true);
            hdfsCluster.shutdown();
        }
    }

    // ------------------------------------------------------------------------

    @Override
    protected FileIO getFileSystem() {
        return fs;
    }

    @Override
    protected Path getBasePath() {
        return basePath;
    }

    @Test
    public void testAtomicWrite() throws IOException {
        Path file = new Path(getBasePath(), randomName());
        fs.tryAtomicOverwriteViaRename(file, "Hi");
        assertThat(fs.readFileUtf8(file)).isEqualTo("Hi");

        fs.tryAtomicOverwriteViaRename(file, "Hello");
        assertThat(fs.readFileUtf8(file)).isEqualTo("Hello");
    }

    @Test
    public void testAtomicWriteMultipleThreads() throws InterruptedException {
        FileIOTest.testOverwriteFileUtf8(new Path(getBasePath(), randomName()), fs);
    }
}
