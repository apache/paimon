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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/** Reader to read lines. */
public interface TextLineReader extends Closeable {

    @Nullable
    String readLine() throws IOException;

    static TextLineReader create(
            InputStream inputStream, String delimiter, long offset, @Nullable Long length)
            throws IOException {
        if (isDefaultDelimiter(delimiter)) {
            return new StandardLineReader(inputStream, offset, length);
        } else {
            if (offset != 0 || length != null) {
                throw new UnsupportedOperationException(
                        "Custom line text file does not support offset and length.");
            }
            return new CustomLineReader(inputStream, delimiter.getBytes(StandardCharsets.UTF_8));
        }
    }

    static boolean isDefaultDelimiter(String delimiter) {
        return delimiter == null
                || "\n".equals(delimiter)
                || "\r\n".equals(delimiter)
                || "\r".equals(delimiter);
    }
}
