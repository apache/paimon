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

package org.apache.paimon.format.text;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A {@link TextLineReader} using {@link BufferedReader} to read by '\n'. */
public class StandardLineReader implements TextLineReader {

    private final InputStream in;
    private final long offset;
    private final byte[] buffer;
    private final @Nullable Long length;
    private final ByteArrayOutputStream lineBuilder;

    private int bufferStart;
    private int bufferEnd;
    private int bufferPosition;
    private boolean endInput;

    public StandardLineReader(InputStream in, long offset, @Nullable Long length)
            throws IOException {
        this.in = in;
        this.offset = offset;
        this.buffer = new byte[8192];
        this.length = length;
        this.lineBuilder = new ByteArrayOutputStream();
        this.endInput = false;
        if (offset == 0) {
            readAtBeginning();
        } else {
            readAtOffset(offset);
        }
    }

    private void readAtBeginning() throws IOException {
        fillBuffer();
        // skipping UTF-8 BOM
        if (bufferEnd >= 3
                && buffer[0] == (byte) 0xEF
                && (buffer[1] == (byte) 0xBB)
                && (buffer[2] == (byte) 0xBF)) {
            bufferStart = 3;
            bufferPosition = 3;
        }
    }

    private void readAtOffset(long offset) throws IOException {
        if (!(in instanceof SeekableInputStream)) {
            throw new IllegalStateException(
                    "Current position only supported for uncompressed files");
        }
        ((SeekableInputStream) in).seek(offset);
        skipFirstLine();
    }

    private void skipFirstLine() throws IOException {
        while (!endInput) {
            if (reachLengthLimit()) {
                endInput();
                return;
            }

            // fill buffer if necessary
            if (bufferPosition >= bufferEnd) {
                fillBuffer();
                if (endInput) {
                    return;
                }
            }

            if (seekToStartOfLineTerminator()) {
                seekPastLineTerminator();
                return;
            }
        }
    }

    @Nullable
    @Override
    public String readLine() throws IOException {
        lineBuilder.reset();

        if (reachLengthLimit()) {
            endInput();
            return null;
        }

        if (bufferPosition >= bufferEnd) {
            fillBuffer();
        }

        while (!endInput) {
            if (seekToStartOfLineTerminator()) {
                copyToLineBuilder();
                seekPastLineTerminator();
                return buildLine();
            }

            checkArgument(bufferPosition == bufferEnd, "expected to be at the end of the buffer");
            copyToLineBuilder();
            fillBuffer();
        }
        String line = buildLine();
        if (line.isEmpty()) {
            return null;
        }
        return line;
    }

    private boolean seekToStartOfLineTerminator() {
        while (bufferPosition < bufferEnd) {
            if (isEndOfLineCharacter(buffer[bufferPosition])) {
                return true;
            }
            bufferPosition++;
        }
        return false;
    }

    private static boolean isEndOfLineCharacter(byte currentByte) {
        return currentByte == '\n' || currentByte == '\r';
    }

    private void seekPastLineTerminator() throws IOException {
        checkArgument(
                isEndOfLineCharacter(buffer[bufferPosition]), "Stream is not at a line terminator");

        // skip carriage return if present
        if (buffer[bufferPosition] == '\r') {
            bufferPosition++;

            // fill buffer if necessary
            if (bufferPosition >= bufferEnd) {
                fillBuffer();
                if (endInput) {
                    return;
                }
            }
        }

        // skip newline if present
        if (buffer[bufferPosition] == '\n') {
            bufferPosition++;
        }
        bufferStart = bufferPosition;
    }

    private void fillBuffer() throws IOException {
        if (endInput) {
            return;
        }
        checkArgument(bufferPosition >= bufferEnd, "Buffer is not empty");
        bufferStart = 0;
        bufferPosition = 0;
        bufferEnd = IOUtils.readNBytes(in, buffer, 0, buffer.length);
        if (bufferEnd == 0) {
            endInput();
        }
    }

    private boolean reachLengthLimit() throws IOException {
        if (length != null) {
            if (!(in instanceof SeekableInputStream)) {
                throw new IllegalStateException(
                        "Current position only supported for uncompressed files");
            }
            int currentBufferSize = bufferEnd - bufferPosition;
            long currentPosition = ((SeekableInputStream) in).getPos() - currentBufferSize;
            return currentPosition > length + offset;
        }
        return false;
    }

    private void copyToLineBuilder() {
        lineBuilder.write(buffer, bufferStart, bufferPosition - bufferStart);
    }

    private String buildLine() throws UnsupportedEncodingException {
        return lineBuilder.toString(UTF_8.name());
    }

    private void endInput() {
        endInput = true;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
