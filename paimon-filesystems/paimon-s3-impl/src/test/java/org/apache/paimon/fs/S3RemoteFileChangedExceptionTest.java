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

import org.apache.hadoop.fs.s3a.RemoteFileChangedException;
import org.junit.jupiter.api.Test;

import static org.apache.paimon.fs.RecordingFileIO.Method.EXISTS;
import static org.apache.paimon.fs.RecordingFileIO.Method.NEW_INPUT_STREAM;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests S3-specific retry behavior of {@link FileIO}. */
class S3RemoteFileChangedExceptionTest {

    @Test
    void overwrittenReadRetriesRemoteFileChangedException() throws Exception {
        RecordingFileIO fileIO = new RecordingFileIO();
        Path path = new Path("s3://bucket/overwritten");
        fileIO.putFile(path, "stable");
        fileIO.failNext(
                NEW_INPUT_STREAM,
                new RemoteFileChangedException(path.toString(), "read", "object changed"));

        assertThat(fileIO.readOverwrittenFileUtf8(path)).contains("stable");
        assertThat(fileIO.callCount(NEW_INPUT_STREAM)).isEqualTo(2);
        assertThat(fileIO.callCount(EXISTS)).isLessThanOrEqualTo(1);
    }
}
