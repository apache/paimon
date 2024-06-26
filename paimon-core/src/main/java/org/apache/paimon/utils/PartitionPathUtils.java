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

import org.apache.paimon.fs.Path;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Utils for file system. */
public class PartitionPathUtils {

    private static final Pattern PARTITION_NAME_PATTERN = Pattern.compile("([^/]+)=([^/]+)");

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

    public static List<String> generatePartitionPaths(
            List<Map<String, String>> partitions, RowType partitionType) {
        return partitions.stream()
                .map(
                        partition ->
                                PartitionPathUtils.generatePartitionPath(partition, partitionType))
                .collect(Collectors.toList());
    }

    public static String generatePartitionPath(
            Map<String, String> partitionSpec, RowType partitionType) {
        LinkedHashMap<String, String> linkedPartitionSpec = new LinkedHashMap<>();
        List<DataField> fields = partitionType.getFields();

        for (DataField dataField : fields) {
            String partitionColumnName = dataField.name();
            String partitionColumnValue = partitionSpec.get(partitionColumnName);
            if (partitionColumnValue != null) {
                linkedPartitionSpec.put(partitionColumnName, partitionColumnValue);
            }
        }

        return generatePartitionPath(linkedPartitionSpec);
    }

    /**
     * Generate all hierarchical paths from partition spec.
     *
     * <p>For example, if the partition spec is (pt1: '0601', pt2: '12', pt3: '30'), this method
     * will return a list (start from index 0):
     *
     * <ul>
     *   <li>pt1=0601
     *   <li>pt1=0601/pt2=12
     *   <li>pt1=0601/pt2=12/pt3=30
     * </ul>
     */
    public static List<String> generateHierarchicalPartitionPaths(
            LinkedHashMap<String, String> partitionSpec) {
        List<String> paths = new ArrayList<>();
        if (partitionSpec.isEmpty()) {
            return paths;
        }
        StringBuilder suffixBuf = new StringBuilder();
        for (Map.Entry<String, String> e : partitionSpec.entrySet()) {
            suffixBuf.append(escapePathName(e.getKey()));
            suffixBuf.append('=');
            suffixBuf.append(escapePathName(e.getValue()));
            suffixBuf.append(Path.SEPARATOR);
            paths.add(suffixBuf.toString());
        }
        return paths;
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

    public static String unescapePathName(String path) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '%' && i + 2 < path.length()) {
                int code = -1;
                try {
                    code = Integer.parseInt(path.substring(i + 1, i + 3), 16);
                } catch (Exception ignored) {
                }
                if (code >= 0) {
                    sb.append((char) code);
                    i += 2;
                    continue;
                }
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Make partition spec from path.
     *
     * @param currPath partition file path.
     * @return Sequential partition specs.
     */
    public static LinkedHashMap<String, String> extractPartitionSpecFromPath(Path currPath) {
        LinkedHashMap<String, String> fullPartSpec = new LinkedHashMap<>();
        List<String[]> kvs = new ArrayList<>();
        do {
            String component = currPath.getName();
            Matcher m = PARTITION_NAME_PATTERN.matcher(component);
            if (m.matches()) {
                String k = unescapePathName(m.group(1));
                String v = unescapePathName(m.group(2));
                String[] kv = new String[2];
                kv[0] = k;
                kv[1] = v;
                kvs.add(kv);
            }
            currPath = currPath.getParent();
        } while (currPath != null && !currPath.getName().isEmpty());

        // reverse the list since we checked the part from leaf dir to table's base dir
        for (int i = kvs.size(); i > 0; i--) {
            fullPartSpec.put(kvs.get(i - 1)[0], kvs.get(i - 1)[1]);
        }

        return fullPartSpec;
    }
}
