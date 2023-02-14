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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.table.store.fs.Path;

import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.Map;

/** Utils for file system. */
public class PartitionPathUtils {

    private static final BitSet CHAR_TO_ESCAPE = new BitSet(128);

    static {
        for (char c = 0; c < ' '; c++) {
            CHAR_TO_ESCAPE.set(c);
        }

        /*
         * ASCII 01-1F are HTTP control characters that need to be escaped.
         * \u000A and \u000D are \n and \r, respectively.
         */
        char[] clist =
                new char[] {
                    '\u0001', '\u0002', '\u0003', '\u0004', '\u0005', '\u0006', '\u0007', '\u0008',
                    '\u0009', '\n', '\u000B', '\u000C', '\r', '\u000E', '\u000F', '\u0010',
                    '\u0011', '\u0012', '\u0013', '\u0014', '\u0015', '\u0016', '\u0017', '\u0018',
                    '\u0019', '\u001A', '\u001B', '\u001C', '\u001D', '\u001E', '\u001F', '"', '#',
                    '%', '\'', '*', '/', ':', '=', '?', '\\', '\u007F', '{', '}', '[', ']', '^'
                };

        for (char c : clist) {
            CHAR_TO_ESCAPE.set(c);
        }
    }

    private static boolean needsEscaping(char c) {
        return c < CHAR_TO_ESCAPE.size() && CHAR_TO_ESCAPE.get(c);
    }

    /**
     * Make partition path from partition spec.
     *
     * @param partitionSpec The partition spec.
     * @return An escaped, valid partition name.
     */
    public static String generatePartitionPath(LinkedHashMap<String, String> partitionSpec) {
        if (partitionSpec.isEmpty()) {
            return "";
        }
        StringBuilder suffixBuf = new StringBuilder();
        int i = 0;
        for (Map.Entry<String, String> e : partitionSpec.entrySet()) {
            if (i > 0) {
                suffixBuf.append(Path.SEPARATOR);
            }
            suffixBuf.append(escapePathName(e.getKey()));
            suffixBuf.append('=');
            suffixBuf.append(escapePathName(e.getValue()));
            i++;
        }
        suffixBuf.append(Path.SEPARATOR);
        return suffixBuf.toString();
    }

    /**
     * Escapes a path name.
     *
     * @param path The path to escape.
     * @return An escaped path name.
     */
    static String escapePathName(String path) {
        if (path == null || path.length() == 0) {
            throw new RuntimeException("Path should not be null or empty: " + path);
        }

        StringBuilder sb = null;
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (needsEscaping(c)) {
                if (sb == null) {
                    sb = new StringBuilder(path.length() + 2);
                    for (int j = 0; j < i; j++) {
                        sb.append(path.charAt(j));
                    }
                }
                escapeChar(c, sb);
            } else if (sb != null) {
                sb.append(c);
            }
        }
        if (sb == null) {
            return path;
        }
        return sb.toString();
    }

    private static void escapeChar(char c, StringBuilder sb) {
        sb.append('%');
        if (c < 16) {
            sb.append('0');
        }
        sb.append(Integer.toHexString(c).toUpperCase());
    }
}
