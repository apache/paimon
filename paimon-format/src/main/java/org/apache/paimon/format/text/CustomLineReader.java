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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/** A {@link TextLineReader} to read lines by custom delimiter. */
public class CustomLineReader implements TextLineReader {

    private static final int MAX_LINE_LENGTH = Integer.MAX_VALUE;

    private final InputStream inputStream;
    private final byte[] delimiter;

    public CustomLineReader(InputStream inputStream, byte[] delimiter) {
        this.inputStream = inputStream;
        this.delimiter = delimiter;
    }

    @Override
    public String readLine() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
        int matchIndex = 0;

        while (true) {
            int b = inputStream.read();
            if (b == -1) {
                // End of stream: flush any partially matched delimiter bytes to output
                if (matchIndex > 0) {
                    out.write(delimiter, 0, matchIndex);
                }
                // Return null if nothing was read, otherwise return the accumulated line
                return out.size() == 0 ? null : out.toString(StandardCharsets.UTF_8.name());
            }

            // Guard against extremely long lines that could cause memory issues
            if (MAX_LINE_LENGTH - matchIndex < out.size()) {
                throw new IOException("Line exceeds maximum length: " + MAX_LINE_LENGTH);
            }

            byte current = (byte) b;
            if (current == delimiter[matchIndex]) {
                // Current byte matches the next expected delimiter byte
                matchIndex++;
                if (matchIndex == delimiter.length) {
                    // Complete delimiter found, return the line without the delimiter
                    return out.toString(StandardCharsets.UTF_8.name());
                }
            } else if (matchIndex > 0) {
                // Mismatch: handle partial matches
                out.write(delimiter, 0, matchIndex);
                if (current == delimiter[0]) {
                    matchIndex = 1;
                } else {
                    out.write(current);
                    matchIndex = 0;
                }
            } else {
                // just add the current byte to output
                out.write(current);
            }
        }
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}
