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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.TypeUtils.castFromString;

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
        return generatePartitionPathUtil(partitionSpec, false);
    }

    public static String generatePartitionPathUtil(
            LinkedHashMap<String, String> partitionSpec, boolean onlyValue) {
        if (partitionSpec.isEmpty()) {
            return "";
        }
        StringBuilder suffixBuf = new StringBuilder();
        int i = 0;
        for (Map.Entry<String, String> e : partitionSpec.entrySet()) {
            if (i > 0) {
                suffixBuf.append(Path.SEPARATOR);
            }
            if (!onlyValue) {
                suffixBuf.append(escapePathName(e.getKey()));
                suffixBuf.append('=');
            }
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
                                PartitionPathUtils.generatePartitionPath(
                                        partition, partitionType, false))
                .collect(Collectors.toList());
    }

    public static String generatePartitionPath(
            Map<String, String> partitionSpec, RowType partitionType, boolean onlyValue) {
        LinkedHashMap<String, String> linkedPartitionSpec = new LinkedHashMap<>();
        List<DataField> fields = partitionType.getFields();

        for (DataField dataField : fields) {
            String partitionColumnName = dataField.name();
            String partitionColumnValue = partitionSpec.get(partitionColumnName);
            if (partitionColumnValue != null) {
                linkedPartitionSpec.put(partitionColumnName, partitionColumnValue);
            }
        }

        return onlyValue
                ? generatePartitionPathUtil(linkedPartitionSpec, true)
                : generatePartitionPath(linkedPartitionSpec);
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

    public static LinkedHashMap<String, String> extractPartitionSpecFromPathOnlyValue(
            Path currPath, List<String> partitionKeys) {
        LinkedHashMap<String, String> fullPartSpec = new LinkedHashMap<>();
        String[] split = currPath.toString().split(Path.SEPARATOR);
        for (int i = 0; i < partitionKeys.size(); i++) {
            fullPartSpec.put(partitionKeys.get(i), split[split.length - partitionKeys.size() + i]);
        }
        return fullPartSpec;
    }

    /**
     * Search all partitions in this path.
     *
     * @param path search path.
     * @param partitionNumber partition number, it will affect path structure.
     * @return all partition specs to its path.
     */
    public static List<Pair<LinkedHashMap<String, String>, Path>> searchPartSpecAndPaths(
            FileIO fileIO,
            Path path,
            int partitionNumber,
            List<String> partitionKeys,
            boolean onlyValueInPath) {
        return searchPartSpecAndPaths(
                fileIO, path, partitionNumber, partitionKeys, onlyValueInPath, null, null, null);
    }

    /**
     * Search all partitions in this path with partition filter support.
     *
     * <p>This method applies the partition filter during directory traversal, which can
     * significantly reduce the number of remote filesystem calls for cloud storage like OSS/S3. For
     * example, if the filter is "ds > '2026' AND ds < '2029'", directories like "ds=2024",
     * "ds=2023" will be skipped without traversing their subdirectories.
     *
     * @param path search path.
     * @param partitionNumber partition number, it will affect path structure.
     * @param partitionKeys partition key names in order.
     * @param onlyValueInPath whether partition path only contains value (not key=value).
     * @param partitionFilter optional predicate to filter partitions during traversal.
     * @param partitionType partition row type, required if partitionFilter is provided.
     * @param defaultPartValue default partition value for null, required if partitionFilter is
     *     provided.
     * @return all partition specs to their paths that match the filter.
     */
    public static List<Pair<LinkedHashMap<String, String>, Path>> searchPartSpecAndPaths(
            FileIO fileIO,
            Path path,
            int partitionNumber,
            List<String> partitionKeys,
            boolean onlyValueInPath,
            @Nullable Predicate partitionFilter,
            @Nullable RowType partitionType,
            @Nullable String defaultPartValue) {
        FileStatus[] generatedParts =
                getFileStatusRecurse(
                        path,
                        partitionNumber,
                        fileIO,
                        partitionKeys,
                        onlyValueInPath,
                        partitionFilter,
                        partitionType,
                        defaultPartValue);
        List<Pair<LinkedHashMap<String, String>, Path>> ret = new ArrayList<>();
        for (FileStatus part : generatedParts) {
            // ignore hidden file
            if (isHiddenFile(part)) {
                continue;
            }
            if (onlyValueInPath) {
                ret.add(
                        Pair.of(
                                extractPartitionSpecFromPathOnlyValue(
                                        part.getPath(), partitionKeys),
                                part.getPath()));
            } else {
                LinkedHashMap<String, String> spec = extractPartitionSpecFromPath(part.getPath());
                if (spec.size() != partitionKeys.size()) {
                    // illegal path, for example: /path/to/table/tmp/unknown, path without "="
                    continue;
                }
                ret.add(Pair.of(spec, part.getPath()));
            }
        }
        return ret;
    }

    private static FileStatus[] getFileStatusRecurse(
            Path path,
            int expectLevel,
            FileIO fileIO,
            List<String> partitionKeys,
            boolean onlyValueInPath,
            @Nullable Predicate partitionFilter,
            @Nullable RowType partitionType,
            @Nullable String defaultPartValue) {
        ArrayList<FileStatus> result = new ArrayList<>();

        try {
            if (fileIO.exists(path)) {
                // ignore hidden file
                FileStatus fileStatus = fileIO.getFileStatus(path);
                // Create an array to hold accumulated partition values at each level
                Object[] partitionValues =
                        partitionFilter != null ? new Object[partitionKeys.size()] : null;
                // Calculate the starting offset when we begin from a prefix path
                // For example, if partitionKeys = [ds, hr] and expectLevel = 1 (only hr remaining),
                // then levelOffset = 2 - 1 = 1, so we access partitionKeys[1] for level 0
                int levelOffset = partitionKeys.size() - expectLevel;
                listStatusRecursively(
                        fileIO,
                        fileStatus,
                        0,
                        expectLevel,
                        result,
                        partitionKeys,
                        onlyValueInPath,
                        partitionFilter,
                        partitionType,
                        defaultPartValue,
                        partitionValues,
                        levelOffset);
            } else {
                return new FileStatus[0];
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to list files in " + path, e);
        }

        return result.toArray(new FileStatus[0]);
    }

    private static void listStatusRecursively(
            FileIO fileIO,
            FileStatus fileStatus,
            int level,
            int expectLevel,
            List<FileStatus> results,
            List<String> partitionKeys,
            boolean onlyValueInPath,
            @Nullable Predicate partitionFilter,
            @Nullable RowType partitionType,
            @Nullable String defaultPartValue,
            @Nullable Object[] partitionValues,
            int levelOffset)
            throws IOException {
        if (isHiddenFile(fileStatus.getPath())) {
            return;
        }

        if (expectLevel == level) {
            results.add(fileStatus);
            return;
        }

        if (fileStatus.isDir()) {
            for (FileStatus stat : fileIO.listStatus(fileStatus.getPath())) {
                // Calculate the actual partition key index considering the level offset
                // When starting from a prefix path, levelOffset accounts for already-traversed
                // levels
                int partitionKeyIndex = levelOffset + level;

                // Apply partition filter if available
                if (partitionFilter != null
                        && partitionType != null
                        && partitionValues != null
                        && partitionKeyIndex < partitionKeys.size()) {
                    // Extract the partition value from the directory name
                    String dirName = stat.getPath().getName();
                    String partitionKey = partitionKeys.get(partitionKeyIndex);
                    String partitionValue;

                    if (onlyValueInPath) {
                        partitionValue = unescapePathName(dirName);
                    } else {
                        // Parse key=value format
                        Matcher m = PARTITION_NAME_PATTERN.matcher(dirName);
                        if (m.matches()) {
                            String key = unescapePathName(m.group(1));
                            if (!key.equals(partitionKey)) {
                                // Key doesn't match expected partition key, skip filtering
                                partitionValue = null;
                            } else {
                                partitionValue = unescapePathName(m.group(2));
                            }
                        } else {
                            // Not a valid partition directory format
                            partitionValue = null;
                        }
                    }

                    if (partitionValue != null) {
                        // Convert the partition value to internal format
                        Object internalValue =
                                defaultPartValue != null && defaultPartValue.equals(partitionValue)
                                        ? null
                                        : castFromString(
                                                partitionValue,
                                                partitionType.getTypeAt(partitionKeyIndex));

                        // Create a copy of partition values and set the current partition key value
                        Object[] currentPartitionValues = partitionValues.clone();
                        currentPartitionValues[partitionKeyIndex] = internalValue;

                        // Build a partial row with the accumulated partition values
                        GenericRow partialRow = new GenericRow(partitionKeys.size());
                        for (int i = 0; i <= partitionKeyIndex; i++) {
                            partialRow.setField(i, currentPartitionValues[i]);
                        }

                        if (!partitionFilter.test(partialRow)) {
                            continue;
                        }

                        // Pass the accumulated values to the next level
                        listStatusRecursively(
                                fileIO,
                                stat,
                                level + 1,
                                expectLevel,
                                results,
                                partitionKeys,
                                onlyValueInPath,
                                partitionFilter,
                                partitionType,
                                defaultPartValue,
                                currentPartitionValues,
                                levelOffset);
                        continue;
                    }
                }

                listStatusRecursively(
                        fileIO,
                        stat,
                        level + 1,
                        expectLevel,
                        results,
                        partitionKeys,
                        onlyValueInPath,
                        partitionFilter,
                        partitionType,
                        defaultPartValue,
                        partitionValues,
                        levelOffset);
            }
        }
    }

    private static boolean isHiddenFile(FileStatus fileStatus) {
        return isHiddenFile(fileStatus.getPath());
    }

    private static boolean isHiddenFile(Path path) {
        String name = path.getName();
        return name.startsWith("_") || name.startsWith(".");
    }
}
