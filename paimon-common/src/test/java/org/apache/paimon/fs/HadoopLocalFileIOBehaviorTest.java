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
import org.apache.hadoop.util.VersionInfo;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;

import static org.assertj.core.api.Assumptions.assumeThat;

/** Behavior tests for Hadoop Local. */
class HadoopLocalFileIOBehaviorTest extends FileIOBehaviorTestBase {

    @TempDir private java.nio.file.Path tmp;

    @Override
    protected FileIO getFileSystem() throws Exception {
        org.apache.hadoop.fs.FileSystem fs = new RawLocalFileSystem();
        fs.initialize(URI.create("file:///"), new Configuration());
        HadoopFileIO fileIO = new HadoopFileIO();
        fileIO.setFileSystem(getBasePath(), fs);
        return fileIO;
    }

    @Override
    protected Path getBasePath() {
        return new Path(tmp.toUri());
    }

    // ------------------------------------------------------------------------

    /** This test needs to be skipped for earlier Hadoop versions because those have a bug. */
    @Override
    protected void testMkdirsFailsForExistingFile() throws Exception {
        final String versionString = VersionInfo.getVersion();
        final String prefix = versionString.substring(0, 3);
        final float version = Float.parseFloat(prefix);
        assumeThat(version)
                .describedAs("Cannot execute this test on Hadoop prior to 2.8")
                .isGreaterThanOrEqualTo(2.8f);

        super.testMkdirsFailsForExistingFile();
    }
}
