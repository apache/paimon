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

package org.apache.paimon.table.format;

import org.apache.paimon.fs.Path;
import org.apache.paimon.table.format.FormatDataSplit.FileMeta;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FormatDataSplit}. */
public class FormatDataSplitTest {

    @Test
    public void testSerializeAndDeserialize() throws IOException, ClassNotFoundException {
        // A split packing one whole file and one offset range of another file.
        FileMeta wholeFile = new FileMeta(new Path("/test/path/file1.parquet"), 1024L);
        FileMeta rangeFile = new FileMeta(new Path("/test/path/file2.csv"), 2048L, 100L, 512L);
        FormatDataSplit split = new FormatDataSplit(Arrays.asList(wholeFile, rangeFile), null);

        byte[] serialized = InstantiationUtil.serializeObject(split);
        FormatDataSplit deserialized =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());

        assertThat(deserialized).isEqualTo(split);
        assertThat(deserialized.files()).isEqualTo(split.files());
        assertThat(deserialized.partition()).isEqualTo(split.partition());
        assertThat(deserialized.fileCount()).isEqualTo(2);
        // readSize: whole file -> fileSize (1024), range -> length (512).
        assertThat(deserialized.totalSize()).isEqualTo(1024L + 512L);

        FileMeta f0 = deserialized.files().get(0);
        assertThat(f0.filePath()).isEqualTo(wholeFile.filePath());
        assertThat(f0.fileSize()).isEqualTo(1024L);
        assertThat(f0.offset()).isEqualTo(0L);
        assertThat(f0.length()).isNull();
        assertThat(f0.readSize()).isEqualTo(1024L);

        FileMeta f1 = deserialized.files().get(1);
        assertThat(f1.filePath()).isEqualTo(rangeFile.filePath());
        assertThat(f1.offset()).isEqualTo(100L);
        assertThat(f1.length()).isEqualTo(512L);
        assertThat(f1.readSize()).isEqualTo(512L);
    }
}
