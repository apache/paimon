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

package org.apache.paimon.fs;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * What io-cache routing knows about the table a FileIO view serves: the locations it owns, the
 * prefixes of its data files and where to find the prefixes of its older schemas.
 */
public final class TableFileIOView implements Serializable {

    private static final long serialVersionUID = 2L;

    public static final TableFileIOView NONE =
            new TableFileIOView(false, Collections.emptyList(), Collections.emptyList(), null);

    private static final TableFileIOView ORIGIN_ONLY =
            new TableFileIOView(true, Collections.emptyList(), Collections.emptyList(), null);

    private final boolean originOnly;
    private final List<String> roots;
    private final List<String> filePrefixes;
    @Nullable private final FilePrefixHistory history;

    private TableFileIOView(
            boolean originOnly,
            List<String> roots,
            List<String> filePrefixes,
            @Nullable FilePrefixHistory history) {
        this.originOnly = originOnly;
        this.roots = roots;
        this.filePrefixes = filePrefixes;
        this.history = history;
    }

    /** The view of a table whose files are under {@code roots}. */
    public static TableFileIOView of(
            Collection<Path> roots,
            Collection<String> filePrefixes,
            @Nullable FilePrefixHistory history) {
        List<String> normalizedRoots = new ArrayList<>();
        for (Path root : roots) {
            addIfAbsent(normalizedRoots, normalizeRoot(root));
        }
        List<String> prefixes = new ArrayList<>();
        for (String prefix : filePrefixes) {
            if (prefix != null && !prefix.trim().isEmpty()) {
                addIfAbsent(prefixes, prefix);
            }
        }
        return new TableFileIOView(
                false,
                Collections.unmodifiableList(normalizedRoots),
                Collections.unmodifiableList(prefixes),
                history);
    }

    /** See {@link IoCacheViewableFileIO#originOnly()}. */
    public TableFileIOView originOnly() {
        return ORIGIN_ONLY;
    }

    /** This view also serving {@code other}'s table, with its history; origin only stays so. */
    public TableFileIOView merge(TableFileIOView other) {
        if (originOnly || other.isNone()) {
            return this;
        }
        if (other.originOnly || isNone()) {
            return other;
        }
        List<String> mergedRoots = new ArrayList<>(roots);
        other.roots.forEach(root -> addIfAbsent(mergedRoots, root));
        List<String> mergedPrefixes = new ArrayList<>(filePrefixes);
        other.filePrefixes.forEach(prefix -> addIfAbsent(mergedPrefixes, prefix));
        TableFileIOView merged =
                new TableFileIOView(
                        false,
                        Collections.unmodifiableList(mergedRoots),
                        Collections.unmodifiableList(mergedPrefixes),
                        other.history == null ? history : other.history);
        return merged.equals(this) ? this : merged;
    }

    public boolean isOriginOnly() {
        return originOnly;
    }

    public boolean isNone() {
        return this == NONE || (!originOnly && roots.isEmpty() && filePrefixes.isEmpty());
    }

    /** Normalized {@code scheme://authority/path} of each location the table owns. */
    public List<String> roots() {
        return roots;
    }

    public List<String> filePrefixes() {
        return filePrefixes;
    }

    @Nullable
    public FilePrefixHistory history() {
        return history;
    }

    /** Whether {@code path} is one of the roots or under one. */
    public static boolean isUnder(List<String> roots, Path path) {
        String location = normalizeRoot(path);
        for (String root : roots) {
            if (location.equals(root) || location.startsWith(root + "/")) {
                return true;
            }
        }
        return false;
    }

    public FileIO apply(FileIO fileIO) {
        if (originOnly) {
            return IoCacheViews.originOnly(fileIO);
        }
        return isNone() ? fileIO : IoCacheViews.withTableView(fileIO, this);
    }

    private static String normalizeRoot(Path path) {
        URI uri = path.toUri();
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        String authority = uri.getAuthority() == null ? "" : uri.getAuthority();
        String location = uri.getPath() == null ? "" : uri.getPath();
        while (location.endsWith("/")) {
            location = location.substring(0, location.length() - 1);
        }
        return scheme + "://" + authority + location;
    }

    private static void addIfAbsent(List<String> list, String value) {
        if (!list.contains(value)) {
            list.add(value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TableFileIOView)) {
            return false;
        }
        TableFileIOView that = (TableFileIOView) o;
        return originOnly == that.originOnly
                && roots.equals(that.roots)
                && filePrefixes.equals(that.filePrefixes)
                && Objects.equals(history, that.history);
    }

    @Override
    public int hashCode() {
        return Objects.hash(originOnly, roots, filePrefixes, history);
    }

    @Override
    public String toString() {
        return originOnly
                ? "TableFileIOView{originOnly}"
                : "TableFileIOView{roots=" + roots + ", prefixes=" + filePrefixes + "}";
    }
}
