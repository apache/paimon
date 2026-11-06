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

package org.apache.paimon.fs.iocache;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * A {@link FileIO} whose io-cache routing can be narrowed to the origin or to one table's files.
 * Internal to io-cache routing, not a public API.
 */
public interface IoCacheViewableFileIO extends FileIO {

    /** A view of this FileIO that sends no request to an io-cache target, for mutable files. */
    FileIO originOnly();

    /** A view of this FileIO for one table: io-cache routing only serves that table's files. */
    FileIO withTableView(TableView view);

    /** {@link #originOnly()} of a FileIO that supports views; any other FileIO as is. */
    static FileIO originOnly(FileIO fileIO) {
        return fileIO instanceof IoCacheViewableFileIO
                ? ((IoCacheViewableFileIO) fileIO).originOnly()
                : fileIO;
    }

    /** {@link #withTableView(TableView)} of a FileIO that supports views; any other as is. */
    static FileIO withTableView(FileIO fileIO, TableView view) {
        return fileIO instanceof IoCacheViewableFileIO
                ? ((IoCacheViewableFileIO) fileIO).withTableView(view)
                : fileIO;
    }

    /** Loads the data file prefixes of the schemas a table has had. */
    interface PrefixHistory extends Serializable {

        /** Reads them through {@code fileIO}, which sends no request to an io-cache target. */
        List<String> filePrefixes(FileIO fileIO) throws IOException;
    }

    /**
     * What io-cache routing knows about the table a FileIO view serves: the locations it owns, the
     * prefixes of its data files and where to find the prefixes of its older schemas.
     */
    final class TableView implements Serializable {

        private static final long serialVersionUID = 1L;

        public static final TableView NONE =
                new TableView(false, Collections.emptyList(), Collections.emptyList(), null);

        private static final TableView ORIGIN_ONLY =
                new TableView(true, Collections.emptyList(), Collections.emptyList(), null);

        private final boolean originOnly;
        private final List<String> roots;
        private final List<String> filePrefixes;
        @Nullable private final PrefixHistory history;

        private TableView(
                boolean originOnly,
                List<String> roots,
                List<String> filePrefixes,
                @Nullable PrefixHistory history) {
            this.originOnly = originOnly;
            this.roots = roots;
            this.filePrefixes = filePrefixes;
            this.history = history;
        }

        /** The view of a table whose files are under {@code roots}. */
        public static TableView of(
                Collection<Path> roots,
                Collection<String> filePrefixes,
                @Nullable PrefixHistory history) {
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
            return new TableView(
                    false,
                    Collections.unmodifiableList(normalizedRoots),
                    Collections.unmodifiableList(prefixes),
                    history);
        }

        /** See {@link IoCacheViewableFileIO#originOnly()}. */
        public TableView originOnly() {
            return ORIGIN_ONLY;
        }

        /** This view also serving {@code other}'s table, with its history; origin only stays so. */
        public TableView merge(TableView other) {
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
            TableView merged =
                    new TableView(
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
        public PrefixHistory history() {
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
                return IoCacheViewableFileIO.originOnly(fileIO);
            }
            return isNone() ? fileIO : IoCacheViewableFileIO.withTableView(fileIO, this);
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
            if (!(o instanceof TableView)) {
                return false;
            }
            TableView that = (TableView) o;
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
                    ? "TableView{originOnly}"
                    : "TableView{roots=" + roots + ", prefixes=" + filePrefixes + "}";
        }
    }
}
