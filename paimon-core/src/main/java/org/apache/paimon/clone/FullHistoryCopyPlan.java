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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Source-to-target file copy plan for full-history clone. */
public class FullHistoryCopyPlan implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<FileCopy> files;

    private FullHistoryCopyPlan(List<FileCopy> files) {
        this.files = Collections.unmodifiableList(new ArrayList<>(files));
    }

    public static FullHistoryCopyPlan empty() {
        return new FullHistoryCopyPlan(Collections.emptyList());
    }

    public static FullHistoryCopyPlan build(FullHistoryFileSet fileSet, PathMapping mapping) {
        List<FileCopy> files = new ArrayList<>();
        Map<String, Path> targetToSource = new HashMap<>();

        try {
            addFiles(
                    files,
                    targetToSource,
                    fileSet.metadataFiles(),
                    FileKind.METADATA,
                    mapping,
                    null);
            addFiles(files, targetToSource, fileSet.dataFiles(), FileKind.DATA, mapping, null);
            addFiles(files, targetToSource, fileSet.indexFiles(), FileKind.INDEX, mapping, null);
        } catch (IOException e) {
            throw new IllegalStateException("Unexpected file size lookup while building plan.", e);
        }

        return new FullHistoryCopyPlan(files);
    }

    public static FullHistoryCopyPlan buildPayload(
            FullHistoryFileSet fileSet, PathMapping mapping, FileIO sourceFileIO)
            throws IOException {
        List<FileCopy> files = new ArrayList<>();
        Map<String, Path> targetToSource = new HashMap<>();
        addFiles(files, targetToSource, fileSet.dataFiles(), FileKind.DATA, mapping, sourceFileIO);
        addFiles(
                files, targetToSource, fileSet.indexFiles(), FileKind.INDEX, mapping, sourceFileIO);
        return new FullHistoryCopyPlan(files);
    }

    private static void addFiles(
            List<FileCopy> files,
            Map<String, Path> targetToSource,
            Iterable<Path> sourceFiles,
            FileKind kind,
            PathMapping mapping,
            FileIO sourceFileIO)
            throws IOException {
        for (Path source : sourceFiles) {
            Path target = new Path(mapping.rewriteRequired(source.toString()));
            checkArgument(
                    !source.equals(target),
                    "Source and target file paths must be different: %s",
                    source);
            Path previousSource = targetToSource.put(target.toString(), source);
            checkArgument(
                    previousSource == null || previousSource.equals(source),
                    "Found target path conflict: source paths %s and %s both map to %s",
                    previousSource,
                    source,
                    target);
            long expectedSize = sourceFileIO == null ? -1L : sourceFileIO.getFileSize(source);
            files.add(new FileCopy(source, target, kind, expectedSize));
        }
    }

    public List<FileCopy> files() {
        return files;
    }

    /** A single source-to-target file copy. */
    public enum FileKind {
        METADATA,
        DATA,
        INDEX
    }

    /** A single source-to-target file copy. */
    public static class FileCopy implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Path source;
        private final Path target;
        private final FileKind kind;
        private final long expectedSize;

        public FileCopy(Path source, Path target) {
            this(source, target, FileKind.METADATA, -1L);
        }

        public FileCopy(Path source, Path target, FileKind kind, long expectedSize) {
            this.source = source;
            this.target = target;
            this.kind = kind;
            this.expectedSize = expectedSize;
        }

        public Path source() {
            return source;
        }

        public Path target() {
            return target;
        }

        public FileKind kind() {
            return kind;
        }

        public long expectedSize() {
            return expectedSize;
        }
    }
}
