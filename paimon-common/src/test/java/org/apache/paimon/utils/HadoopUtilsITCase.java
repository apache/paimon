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
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HadoopUtils}. */
public class HadoopUtilsITCase {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testHadoopConfDirWithScheme() throws Exception {
        LocalFileIO.create()
                .overwriteFileUtf8(
                        new Path(tempDir.toString(), "core-site.xml"),
                        "<property>\n"
                                + "  <name>paimon.test.key</name>\n"
                                + "  <value>test.value</value>\n"
                                + "</property>");

        Options options = new Options();
        options.set(
                HadoopUtils.PATH_HADOOP_CONFIG,
                TestFileIOLoader.SCHEME + "://" + tempDir.toString());
        Configuration conf = HadoopUtils.getHadoopConfiguration(options);
        assertThat(conf.get("paimon.test.key")).isEqualTo("test.value");
    }

    public static class TestFileIOLoader implements FileIOLoader {

        private static final String SCHEME = "hadoop-utils-test-file-io";

        @Override
        public String getScheme() {
            return SCHEME;
        }

        @Override
        public FileIO load(Path path) {
            return LocalFileIO.create();
        }
    }
}
