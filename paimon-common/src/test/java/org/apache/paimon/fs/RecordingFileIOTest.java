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

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.paimon.fs.RecordingFileIO.Method.NEW_INPUT_STREAM;
import static org.apache.paimon.fs.RecordingFileIO.Method.NEW_OUTPUT_STREAM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RecordingFileIO}. */
class RecordingFileIOTest {

    @Test
    void recordsTypedPrimitiveArgumentsWithoutLosingFileStateOnReset() throws Exception {
        RecordingFileIO fileIO = new RecordingFileIO();
        Path path = new Path("test:///data/value.txt");
        fileIO.putFile(path, "old");

        fileIO.reset();
        fileIO.writeFile(path, "new", true);

        assertThat(fileIO.fileContent(path)).isEqualTo("new");
        assertThat(fileIO.calls())
                .containsExactly(RecordingFileIO.call(NEW_OUTPUT_STREAM, path, true));
    }

    @Test
    void scriptsOneShotPrimitiveFailuresAndResetClearsThem() throws Exception {
        RecordingFileIO fileIO = new RecordingFileIO();
        Path path = new Path("test:///data/value.txt");
        fileIO.putFile(path, "value");
        fileIO.failNext(NEW_INPUT_STREAM, new IOException("planned"));

        assertThatThrownBy(() -> fileIO.readFileUtf8(path))
                .isInstanceOf(IOException.class)
                .hasMessage("planned");
        assertThat(fileIO.calls()).containsExactly(RecordingFileIO.call(NEW_INPUT_STREAM, path));

        fileIO.failNext(NEW_INPUT_STREAM, new IOException("cleared"));
        fileIO.reset();
        assertThat(fileIO.readFileUtf8(path)).isEqualTo("value");
    }

    @Test
    void tracksOpenStreamsUntilTheyAreClosed() throws Exception {
        RecordingFileIO fileIO = new RecordingFileIO();
        Path source = new Path("test:///data/source.txt");
        Path target = new Path("test:///data/target.txt");
        fileIO.putFile(source, "value");

        SeekableInputStream input = fileIO.newInputStream(source);
        PositionOutputStream output = fileIO.newOutputStream(target, false);
        assertThat(fileIO.openInputStreams()).isEqualTo(1);
        assertThat(fileIO.openOutputStreams()).isEqualTo(1);

        input.close();
        output.close();
        assertThat(fileIO.openInputStreams()).isZero();
        assertThat(fileIO.openOutputStreams()).isZero();
    }
}
