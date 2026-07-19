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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
            String value = e.getValue();
            validatePartitionValueForPath(value, onlyValue);
            suffixBuf.append(escapePathName(value));
            i++;
        }
        suffixBuf.append(Path.SEPARATOR);
        return suffixBuf.toString();
    }

    /**
     * Generate a partition path without the trailing separator, e.g. {@code dt=20250101/hr=01}.
     * This is the canonical partition name used when talking to a partition-managing catalog.
     */
    public static String generatePartitionName(
            LinkedHashMap<String, String> partitionSpec, boolean onlyValue) {
        String path = generatePartitionPathUtil(partitionSpec, onlyValue);
        return path.endsWith(Path.SEPARATOR)
                ? path.substring(0, path.length() - Path.SEPARATOR.length())
                : path;
    }

    /**
     * Validate that a partition value is safe for the configured path layout. In a key-value
     * layout, values such as {@code "."} are part of a component such as {@code "pt=."} and are
     * safe. In a value-only layout, {@code "."} and {@code ".."} are complete path components and
     * would resolve to a different directory.
     */
    public static void validatePartitionValueForPath(String value, boolean onlyValueInPath) {
        if (value == null
                || value.isEmpty()
                || (onlyValueInPath && (".".equals(value) || "..".equals(value)))) {
            throw new IllegalArgumentException(
                    String.format(
                            "Partition value '%s' cannot be used as a partition path component.",
                            value));
        }
    }

    /** Conservatively validate a value when the physical partition layout is unknown. */
    public static void validatePartitionValueForPath(String value) {
        validatePartitionValueForPath(value, true);
    }

    /** Validate every value of a partition spec for the configured path layout. */
    public static void validatePartitionSpecForPath(
            Map<String, String> partitionSpec, boolean onlyValueInPath) {
        for (String value : partitionSpec.values()) {
            validatePartitionValueForPath(value, onlyValueInPath);
        }
    }

    /** Conservatively validate a spec when the physical partition layout is unknown. */
    public static void validatePartitionSpecForPath(Map<String, String> partitionSpec) {
        validatePartitionSpecForPath(partitionSpec, true);
    }

    /**
     * Build the partition-name prefix pattern pushed down to a partition-managing catalog from the
     * leading equality prefix of a partition predicate.
     *
     * <p>Pattern contract (shared by every engine talking to the catalog): partition names are the
     * escaped {@code key=value} form joined by {@code '/'}; {@code '%'} is the only wildcard and
     * there is no escape sequence for it ({@code '_'} stays a literal). A complete spec matches the
     * exact partition name; an incomplete prefix is suffixed with {@code '%'}.
     *
     * <p>Returns {@code null} whenever pushdown must be skipped and the caller should list all
     * partitions instead: the equality prefix is empty, a prefix value is blank, or the escaped
     * prefix contains a literal {@code '%'} that the contract cannot express.
     */
    @Nullable
    public static String buildPartitionNamePrefixPattern(
            List<String> partitionKeys, Map<String, String> equalityPrefix) {
        if (equalityPrefix.isEmpty()) {
            return null;
        }
        LinkedHashMap<String, String> orderedPrefix = new LinkedHashMap<>();
        for (String partitionKey : partitionKeys) {
            if (!equalityPrefix.containsKey(partitionKey)) {
                break;
            }
            String value = equalityPrefix.get(partitionKey);
            if (StringUtils.isNullOrWhitespaceOnly(value)) {
                return null;
            }
            orderedPrefix.put(partitionKey, value);
        }
        if (orderedPrefix.isEmpty()) {
            return null;
        }
        String escapedPrefix = generatePartitionPath(orderedPrefix);
        if (escapedPrefix.indexOf('%') >= 0) {
            return null;
        }
        if (orderedPrefix.size() == partitionKeys.size()) {
            return escapedPrefix.substring(0, escapedPrefix.length() - 1);
        }
        return escapedPrefix + '%';
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

    /** Extract exactly the trailing key-value components for the declared partition keys. */
    @Nullable
    static LinkedHashMap<String, String> extractPartitionSpecFromPath(
            Path currPath, List<String> partitionKeys) {
        String[] values = new String[partitionKeys.size()];
        Path current = currPath;
        for (int i = partitionKeys.size() - 1; i >= 0; i--) {
            if (current == null) {
                return null;
            }
            Matcher matcher = PARTITION_NAME_PATTERN.matcher(current.getName());
            if (!matcher.matches()
                    || !partitionKeys.get(i).equals(unescapePathName(matcher.group(1)))) {
                return null;
            }
            values[i] = unescapePathName(matcher.group(2));
            current = current.getParent();
        }

        LinkedHashMap<String, String> spec = new LinkedHashMap<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            spec.put(partitionKeys.get(i), values[i]);
        }
        return spec;
    }

    public static LinkedHashMap<String, String> extractPartitionSpecFromPathOnlyValue(
            Path currPath, List<String> partitionKeys) {
        LinkedHashMap<String, String> fullPartSpec = new LinkedHashMap<>();
        String[] split = currPath.toString().split(Path.SEPARATOR);
        for (int i = 0; i < partitionKeys.size(); i++) {
            // Unescape the directory component so the extracted value is the RAW partition value,
            // consistent with the key=value branch (extractPartitionSpecFromPath) and with the
            // values the write path registers into a partition-managing catalog. Without this,
            // directories containing escaped characters (e.g. a%3Ab) would round-trip to a
            // different value than the one registered (a:b).
            fullPartSpec.put(
                    partitionKeys.get(i),
                    unescapePathName(split[split.length - partitionKeys.size() + i]));
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
                fileIO,
                path,
                partitionNumber,
                partitionKeys,
                onlyValueInPath,
                (Predicate) null,
                null,
                null);
    }

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
            if (isHiddenFile(part, onlyValueInPath, defaultPartValue)) {
                continue;
            }
            if (onlyValueInPath) {
                ret.add(
                        Pair.of(
                                extractPartitionSpecFromPathOnlyValue(
                                        part.getPath(), partitionKeys),
                                part.getPath()));
            } else {
                LinkedHashMap<String, String> spec =
                        extractPartitionSpecFromPath(part.getPath(), partitionKeys);
                if (spec == null) {
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

        // Only predicate-referenced levels are parsed/pruned.
        Set<String> referencedFields =
                partitionFilter == null
                        ? Collections.emptySet()
                        : PredicateVisitor.collectFieldNames(partitionFilter);
        GenericRow values =
                partitionType == null ? null : new GenericRow(partitionType.getFieldCount());

        FileStatus fileStatus;
        try {
            fileStatus = fileIO.getFileStatus(path);
        } catch (FileNotFoundException e) {
            // A missing root simply means the table has no partitions yet.
            return new FileStatus[0];
        } catch (IOException e) {
            throw new RuntimeException("Failed to list files in " + path, e);
        }

        try {
            // Skip partition levels already fixed by the scan-path prefix.
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
                    referencedFields,
                    partitionType,
                    defaultPartValue,
                    levelOffset,
                    values);
        } catch (IOException e) {
            // Never degrade a mid-scan failure into an empty listing: callers diff this result
            // against partition metadata and an incomplete listing would deregister partitions
            // that still exist.
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
            Set<String> referencedFields,
            @Nullable RowType partitionType,
            @Nullable String defaultPartValue,
            int levelOffset,
            @Nullable GenericRow values)
            throws IOException {
        if (isHiddenFile(fileStatus, onlyValueInPath, defaultPartValue)) {
            return;
        }

        if (expectLevel == level) {
            results.add(fileStatus);
            return;
        }

        if (fileStatus.isDir()) {
            FileStatus[] children;
            try {
                children = fileIO.listStatus(fileStatus.getPath());
            } catch (FileNotFoundException e) {
                // The directory vanished after the parent listed it: the partitions beneath it
                // are gone, skipping just this subtree keeps the rest of the listing complete.
                return;
            }
            for (FileStatus stat : children) {
                int partitionKeyIndex = levelOffset + level;
                String partitionKey = partitionKeys.get(partitionKeyIndex);

                // Bind the current partition value and prune only when the partially bound
                // predicate is provably false. Unreferenced or unparseable levels are descended
                // without pruning.
                if (partitionFilter != null
                        && partitionType != null
                        && values != null
                        && referencedFields.contains(partitionKey)) {
                    String partitionValue =
                            parsePartitionValue(
                                    stat.getPath().getName(), partitionKey, onlyValueInPath);
                    if (partitionValue != null) {
                        Object internalValue =
                                defaultPartValue != null && defaultPartValue.equals(partitionValue)
                                        ? null
                                        : castFromString(
                                                partitionValue,
                                                partitionType.getTypeAt(partitionKeyIndex));
                        values.setField(partitionKeyIndex, internalValue);
                        if (!mightMatch(partitionFilter, levelOffset, partitionKeyIndex, values)) {
                            continue;
                        }
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
                        referencedFields,
                        partitionType,
                        defaultPartValue,
                        levelOffset,
                        values);
            }
        }
    }

    /** Returns the partition value from a directory name, or {@code null} when it can't be used. */
    @Nullable
    private static String parsePartitionValue(
            String dirName, String partitionKey, boolean onlyValueInPath) {
        if (onlyValueInPath) {
            return unescapePathName(dirName);
        }
        Matcher m = PARTITION_NAME_PATTERN.matcher(dirName);
        if (m.matches()) {
            String key = unescapePathName(m.group(1));
            if (!key.equals(partitionKey)) {
                return null;
            }
            return unescapePathName(m.group(2));
        }
        return null;
    }

    /**
     * Returns whether a subtree might still match the partition predicate.
     *
     * <p>{@code values} holds the currently bound partition values in {@code [minIdx, maxIdx]}.
     * Indices below {@code minIdx} belong to the scan-path prefix; indices above {@code maxIdx} are
     * not known yet. Returning {@code false} means the subtree can be pruned safely.
     */
    static boolean mightMatch(
            @Nullable Predicate predicate, int minIdx, int maxIdx, InternalRow values) {
        if (predicate == null) {
            return true;
        }
        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compound = (CompoundPredicate) predicate;
            if (compound.function() instanceof Or) {
                for (Predicate child : compound.children()) {
                    if (mightMatch(child, minIdx, maxIdx, values)) {
                        return true;
                    }
                }
                return false;
            }
            if (compound.function() instanceof And) {
                for (Predicate child : compound.children()) {
                    if (!mightMatch(child, minIdx, maxIdx, values)) {
                        return false;
                    }
                }
                return true;
            }
            return true;
        }
        if (predicate instanceof LeafPredicate) {
            for (Object input : ((LeafPredicate) predicate).transform().inputs()) {
                if (input instanceof FieldRef) {
                    int idx = ((FieldRef) input).index();
                    if (idx < minIdx || idx > maxIdx) {
                        return true;
                    }
                }
            }
            return predicate.test(values);
        }
        // Unknown predicate node: be conservative.
        return true;
    }

    private static boolean isHiddenFile(
            FileStatus fileStatus, boolean onlyValueInPath, @Nullable String defaultPartValue) {
        return isHiddenFile(fileStatus.getPath(), onlyValueInPath, defaultPartValue);
    }

    private static boolean isHiddenFile(
            Path path, boolean onlyValueInPath, @Nullable String defaultPartValue) {
        String name = path.getName();
        if (onlyValueInPath && defaultPartValue != null && defaultPartValue.equals(name)) {
            return false;
        }
        return name.startsWith("_") || name.startsWith(".");
    }
}
