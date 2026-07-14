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

package org.apache.paimon.clone;

import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.StringUtils;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Prefix mappings used to rewrite absolute paths for full-history clone. */
public class PathMapping implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<Entry> entries;

    private PathMapping(List<Entry> entries) {
        this.entries = entries;
    }

    public static PathMapping parse(List<String> mappings) {
        checkArgument(mappings != null && !mappings.isEmpty(), "Path mappings must not be empty.");

        List<Entry> entries = new ArrayList<>();
        Map<String, String> seenSources = new HashMap<>();
        for (String mapping : mappings) {
            checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(mapping) && mapping.indexOf('=') > 0,
                    "Path mapping must be in source=target format: %s",
                    mapping);
            int split = mapping.indexOf('=');
            String sourcePrefix = normalizePrefix(mapping.substring(0, split));
            String targetPrefix = normalizePrefix(mapping.substring(split + 1));
            checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(sourcePrefix)
                            && !StringUtils.isNullOrWhitespaceOnly(targetPrefix),
                    "Path mapping must be in source=target format: %s",
                    mapping);
            String previousTarget = seenSources.put(sourcePrefix, targetPrefix);
            checkArgument(
                    previousTarget == null,
                    "Duplicate path mapping source prefix: %s",
                    sourcePrefix);
            entries.add(new Entry(sourcePrefix, targetPrefix));
        }

        for (Entry targetEntry : entries) {
            for (Entry sourceEntry : entries) {
                checkArgument(
                        !overlaps(targetEntry.targetPrefix, sourceEntry.sourcePrefix),
                        "Source and target path mapping prefixes must not overlap: %s and %s",
                        sourceEntry.sourcePrefix,
                        targetEntry.targetPrefix);
            }
        }
        for (int i = 0; i < entries.size(); i++) {
            for (int j = i + 1; j < entries.size(); j++) {
                checkArgument(
                        !overlaps(entries.get(i).targetPrefix, entries.get(j).targetPrefix),
                        "Target path mapping prefixes must not overlap: %s and %s",
                        entries.get(i).targetPrefix,
                        entries.get(j).targetPrefix);
            }
        }

        Collections.sort(
                entries,
                new Comparator<Entry>() {
                    @Override
                    public int compare(Entry left, Entry right) {
                        return Integer.compare(
                                right.sourcePrefix.length(), left.sourcePrefix.length());
                    }
                });
        return new PathMapping(Collections.unmodifiableList(entries));
    }

    public Optional<String> rewrite(String sourcePath) {
        String normalizedSourcePath = new Path(sourcePath).toString();
        for (Entry entry : entries) {
            if (entry.matches(normalizedSourcePath)) {
                String suffix = normalizedSourcePath.substring(entry.sourcePrefix.length());
                if (entry.targetPrefix.endsWith("/") && suffix.startsWith("/")) {
                    suffix = suffix.substring(1);
                }
                return Optional.of(entry.targetPrefix + suffix);
            }
        }
        return Optional.empty();
    }

    public String rewriteRequired(String sourcePath) {
        Optional<String> rewritten = rewrite(sourcePath);
        checkArgument(rewritten.isPresent(), "No path mapping matched source path: %s", sourcePath);
        return rewritten.get();
    }

    public Map<String, String> rewriteAllRequired(Collection<String> sourcePaths) {
        Map<String, String> result = new LinkedHashMap<>();
        Map<String, String> targetToSource = new HashMap<>();
        for (String sourcePath : sourcePaths) {
            String targetPath = rewriteRequired(sourcePath);
            String previousSource = targetToSource.put(targetPath, sourcePath);
            checkArgument(
                    previousSource == null || previousSource.equals(sourcePath),
                    "Found target path conflict: source paths %s and %s both map to %s",
                    previousSource,
                    sourcePath,
                    targetPath);
            result.put(sourcePath, targetPath);
        }
        return result;
    }

    public String identity() {
        List<String> mappings = new ArrayList<>();
        for (Entry entry : entries) {
            mappings.add(entry.sourcePrefix + "=" + entry.targetPrefix);
        }
        Collections.sort(mappings);
        return String.join("\n", mappings);
    }

    private static String normalizePrefix(String prefix) {
        String normalized = new Path(prefix.trim()).toString();
        URI uri = new Path(normalized).toUri();
        checkArgument(
                uri.getQuery() == null && uri.getFragment() == null,
                "Path mapping prefixes must not contain a query or fragment: %s",
                prefix);
        while (normalized.length() > 1
                && normalized.endsWith("/")
                && !normalized.matches("^[A-Za-z][A-Za-z0-9+.-]*:/+$")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    static boolean overlaps(String left, String right) {
        return isSameOrDescendant(left, right) || isSameOrDescendant(right, left);
    }

    private static boolean isSameOrDescendant(String path, String parent) {
        URI pathUri = new Path(path).toUri();
        URI parentUri = new Path(parent).toUri();
        if (!equalsIgnoreCase(pathUri.getScheme(), parentUri.getScheme())
                || !equalsIgnoreCase(pathUri.getAuthority(), parentUri.getAuthority())) {
            return false;
        }
        String pathPart = pathUri.getPath();
        String parentPart = parentUri.getPath();
        return pathPart.equals(parentPart)
                || (parentPart.endsWith("/") && pathPart.startsWith(parentPart))
                || pathPart.startsWith(parentPart + "/");
    }

    private static boolean equalsIgnoreCase(String left, String right) {
        return left == null ? right == null : right != null && left.equalsIgnoreCase(right);
    }

    private static class Entry implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String sourcePrefix;
        private final String targetPrefix;

        private Entry(String sourcePrefix, String targetPrefix) {
            this.sourcePrefix = sourcePrefix;
            this.targetPrefix = targetPrefix;
        }

        private boolean matches(String sourcePath) {
            return sourcePath.equals(sourcePrefix)
                    || (sourcePrefix.endsWith("/") && sourcePath.startsWith(sourcePrefix))
                    || sourcePath.startsWith(sourcePrefix + "/");
        }
    }
}
