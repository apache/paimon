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

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/** A {@link TextLineReader} to read lines by custom delimiter. */
public class CustomLineReader implements TextLineReader {

    private static final int MAX_LINE_LENGTH = Integer.MAX_VALUE;

    private final InputStream inputStream;
    private final byte[] delimiter;
    private final int[] prefixTable;

    public CustomLineReader(InputStream inputStream, byte[] delimiter) {
        this.inputStream = inputStream;
        this.delimiter = delimiter;
        this.prefixTable = buildPrefixTable(delimiter);
    }

    @Nullable
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
            while (matchIndex > 0 && current != delimiter[matchIndex]) {
                int fallback = prefixTable[matchIndex - 1];
                out.write(delimiter, 0, matchIndex - fallback);
                matchIndex = fallback;
            }

            if (current == delimiter[matchIndex]) {
                // Current byte matches the next expected delimiter byte
                matchIndex++;
                if (matchIndex == delimiter.length) {
                    // Complete delimiter found, return the line without the delimiter
                    return out.toString(StandardCharsets.UTF_8.name());
                }
            } else {
                // just add the current byte to output
                out.write(current);
            }
        }
    }

    private static int[] buildPrefixTable(byte[] delimiter) {
        int[] table = new int[delimiter.length];
        int matched = 0;
        for (int i = 1; i < delimiter.length; i++) {
            while (matched > 0 && delimiter[i] != delimiter[matched]) {
                matched = table[matched - 1];
            }
            if (delimiter[i] == delimiter[matched]) {
                matched++;
            }
            table[i] = matched;
        }
        return table;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}
