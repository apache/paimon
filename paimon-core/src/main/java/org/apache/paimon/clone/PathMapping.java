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

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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
        for (int i = 0; i < mappings.size(); i++) {
            String mapping = mappings.get(i);
            checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(mapping) && mapping.indexOf('=') > 0,
                    "Path mapping at index %s must be in source=target format.",
                    i);
            int split = mapping.indexOf('=');
            String sourcePrefix = normalizePrefix(mapping.substring(0, split));
            String targetPrefix = normalizePrefix(mapping.substring(split + 1));
            checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(sourcePrefix)
                            && !StringUtils.isNullOrWhitespaceOnly(targetPrefix),
                    "Path mapping at index %s must be in source=target format.",
                    i);
            String previousTarget = seenSources.put(sourcePrefix, targetPrefix);
            checkArgument(
                    previousTarget == null,
                    "Duplicate path mapping source prefix: %s",
                    sourcePrefix);
            for (Entry entry : entries) {
                checkArgument(
                        !samePath(sourcePrefix, entry.sourcePrefix),
                        "Duplicate path mapping source prefix: %s",
                        sourcePrefix);
            }
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
                        return Integer.compare(right.sourcePathLength(), left.sourcePathLength());
                    }
                });
        return new PathMapping(Collections.unmodifiableList(entries));
    }

    public Optional<String> rewrite(String sourcePath) {
        String normalizedSourcePath = normalizeRuntimePath(sourcePath);
        for (Entry entry : entries) {
            if (entry.matches(normalizedSourcePath)) {
                String suffix = relativeSuffix(normalizedSourcePath, entry.sourcePrefix);
                String rewritten = appendSuffix(entry.targetPrefix, suffix);
                checkArgument(
                        isSameOrDescendant(rewritten, entry.targetPrefix),
                        "Rewritten path escaped target prefix %s: %s",
                        entry.targetPrefix,
                        rewritten);
                return Optional.of(rewritten);
            }
        }
        return Optional.empty();
    }

    public String rewriteRequired(String sourcePath) {
        Optional<String> rewritten = rewrite(sourcePath);
        checkArgument(rewritten.isPresent(), "No path mapping matched source path: %s", sourcePath);
        return rewritten.get();
    }

    public String rewriteRequiredUnder(String sourcePath, String sourceAnchor) {
        String normalizedSource = normalizeRuntimePath(sourcePath);
        String normalizedAnchor = normalizeRuntimePath(sourceAnchor);
        checkArgument(
                isSameOrDescendant(normalizedSource, normalizedAnchor),
                "Source path %s is not under mapping anchor %s.",
                sourcePath,
                sourceAnchor);

        String targetAnchor = rewriteRequired(normalizedAnchor);
        String suffix = relativeSuffix(normalizedSource, normalizedAnchor);
        return appendSuffix(targetAnchor, suffix);
    }

    public Map<String, String> rewriteAllRequired(Collection<String> sourcePaths) {
        Map<String, String> result = new LinkedHashMap<>();
        Map<String, String> targetToSource = new HashMap<>();
        for (String sourcePath : sourcePaths) {
            String targetPath = rewriteRequired(sourcePath);
            String previousSource =
                    targetToSource.put(conflictKey(new Path(targetPath)), sourcePath);
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

    List<String> mappedTargetPrefixesUnder(String sourceRoot) {
        String normalizedRoot = normalizeRuntimePath(sourceRoot);
        List<String> result = new ArrayList<>();
        result.add(rewriteRequired(normalizedRoot));
        for (Entry entry : entries) {
            if (isSameOrDescendant(entry.sourcePrefix, normalizedRoot)
                    && !result.contains(entry.targetPrefix)) {
                result.add(entry.targetPrefix);
            }
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

    /** Returns a conservative key for detecting paths which may alias on a local file system. */
    public static String conflictKey(Path path) {
        URI uri = path.toUri();
        if (isLocalAbsolutePath(uri)) {
            return "local:" + uri.getPath().toLowerCase(Locale.ROOT);
        }
        return path.toString();
    }

    private static String normalizePrefix(String prefix) {
        String normalized = new Path(prefix.trim()).toString();
        URI uri = new Path(normalized).toUri();
        checkArgument(
                uri.getQuery() == null && uri.getFragment() == null,
                "Path mapping prefixes must not contain a query or fragment.");
        checkArgument(
                uri.getUserInfo() == null, "Path mapping prefixes must not contain user info.");
        checkArgument(
                !((uri.getScheme() == null || "file".equalsIgnoreCase(uri.getScheme()))
                        && uri.getAuthority() != null),
                "Local path mapping prefix must not include authority: %s",
                normalized);
        checkArgument(
                !((uri.getScheme() == null || "file".equalsIgnoreCase(uri.getScheme()))
                        && !uri.getPath().startsWith("/")),
                "Local path mapping prefix must be absolute: %s",
                normalized);
        checkArgument(
                uri.getScheme() == null
                        || "file".equalsIgnoreCase(uri.getScheme())
                        || uri.getAuthority() != null,
                "Non-local path mapping prefix must include authority: %s",
                normalized);
        while (normalized.length() > 1
                && normalized.endsWith("/")
                && !normalized.matches("^[A-Za-z][A-Za-z0-9+.-]*:/+$")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private static String normalizeRuntimePath(String path) {
        Path normalized = new Path(path);
        checkArgument(
                normalized.toUri().getUserInfo() == null,
                "Source paths and mapping anchors must not contain user info.");
        return normalized.toString();
    }

    private static String appendSuffix(String prefix, String suffix) {
        if (suffix.isEmpty()) {
            return new Path(prefix).toString();
        }
        if (prefix.endsWith("/") && suffix.startsWith("/")) {
            suffix = suffix.substring(1);
        } else if (!prefix.endsWith("/") && !suffix.startsWith("/")) {
            suffix = "/" + suffix;
        }
        return new Path(prefix + suffix).toString();
    }

    private static String relativeSuffix(String path, String parent) {
        String pathPart = new Path(path).toUri().getPath();
        String parentPart = new Path(parent).toUri().getPath();
        return pathPart.substring(parentPart.length());
    }

    static boolean overlaps(String left, String right) {
        return isSameOrDescendantForMappingPrefix(left, right)
                || isSameOrDescendantForMappingPrefix(right, left);
    }

    private static boolean samePath(String left, String right) {
        return isSameOrDescendantForMappingPrefix(left, right)
                && isSameOrDescendantForMappingPrefix(right, left);
    }

    static boolean isSameOrDescendant(String path, String parent) {
        return isSameOrDescendant(path, parent, false, false);
    }

    static boolean isSameOrDescendantForConflict(String path, String parent) {
        return isSameOrDescendant(path, parent, true, false);
    }

    private static boolean isSameOrDescendantForMappingPrefix(String path, String parent) {
        return isSameOrDescendant(path, parent, true, true);
    }

    private static boolean isSameOrDescendant(
            String path,
            String parent,
            boolean allowLocalAlias,
            boolean resolveLocalSymbolicLinks) {
        URI pathUri = new Path(path).toUri();
        URI parentUri = new Path(parent).toUri();
        if (!(allowLocalAlias
                        ? sameSchemeForOverlap(pathUri, parentUri)
                        : equalsIgnoreCase(pathUri.getScheme(), parentUri.getScheme()))
                || !equalsIgnoreCase(pathUri.getAuthority(), parentUri.getAuthority())) {
            return false;
        }
        String pathPart = pathUri.getPath();
        String parentPart = parentUri.getPath();
        if (allowLocalAlias && isLocalAbsolutePath(pathUri) && isLocalAbsolutePath(parentUri)) {
            if (resolveLocalSymbolicLinks) {
                pathPart = canonicalLocalPath(pathUri);
                parentPart = canonicalLocalPath(parentUri);
            }
            pathPart = pathPart.toLowerCase(Locale.ROOT);
            parentPart = parentPart.toLowerCase(Locale.ROOT);
        }
        return pathPart.equals(parentPart)
                || (parentPart.endsWith("/") && pathPart.startsWith(parentPart))
                || pathPart.startsWith(parentPart + "/");
    }

    private static String canonicalLocalPath(URI uri) {
        try {
            java.nio.file.Path localPath =
                    uri.getScheme() == null ? Paths.get(uri.getPath()) : Paths.get(uri);
            java.nio.file.Path existingAncestor = localPath;
            while (existingAncestor != null
                    && !Files.exists(existingAncestor, LinkOption.NOFOLLOW_LINKS)) {
                existingAncestor = existingAncestor.getParent();
            }
            checkArgument(
                    existingAncestor != null,
                    "Local path mapping prefix has no existing ancestor: %s",
                    uri);
            java.nio.file.Path suffix = existingAncestor.relativize(localPath);
            return existingAncestor.toRealPath().resolve(suffix).normalize().toUri().getPath();
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Failed to resolve local path mapping prefix: " + uri, e);
        }
    }

    private static boolean equalsIgnoreCase(String left, String right) {
        return left == null ? right == null : right != null && left.equalsIgnoreCase(right);
    }

    private static boolean sameSchemeForOverlap(URI left, URI right) {
        if (equalsIgnoreCase(left.getScheme(), right.getScheme())) {
            return true;
        }
        return isLocalAbsolutePath(left) && isLocalAbsolutePath(right);
    }

    private static boolean isLocalAbsolutePath(URI uri) {
        return (uri.getScheme() == null || "file".equalsIgnoreCase(uri.getScheme()))
                && uri.getAuthority() == null
                && uri.getPath().startsWith("/");
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
            return isSameOrDescendant(sourcePath, sourcePrefix);
        }

        private int sourcePathLength() {
            return new Path(sourcePrefix).toUri().getPath().length();
        }
    }
}
