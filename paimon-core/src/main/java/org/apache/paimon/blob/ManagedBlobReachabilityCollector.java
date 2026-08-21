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

package org.apache.paimon.blob;

import org.apache.paimon.blob.ManagedBlobReferenceFile.Reference;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Collects managed BLOB pack reachability from data-file {@code .blobref} sidecars.
 *
 * <p>This collector does not scan snapshots or delete files. Callers such as orphan-file cleanup
 * (and later snapshot expiration) supply data files and decide what to delete from {@link Result}.
 */
public class ManagedBlobReachabilityCollector {

    private static final Logger LOG =
            LoggerFactory.getLogger(ManagedBlobReachabilityCollector.class);

    private static final int READ_RETRY_NUM = 3;
    private static final int READ_RETRY_INTERVAL_MS = 5;

    private final FileIO fileIO;

    public ManagedBlobReachabilityCollector(FileIO fileIO) {
        this.fileIO = fileIO;
    }

    /**
     * Reads blobref extras of one data file. Extra files without a {@code .blobref} suffix are
     * ignored. A listed sidecar that cannot be trusted marks the result unsafe, unless the data
     * file itself is already gone: unmerged snapshot manifests can still contain {@code ADD}
     * entries that snapshot expire has deleted, and those must not abort pack GC.
     */
    public Result fromDataFile(Path dataFile, List<String> extraFiles) {
        Result result = Result.empty();
        if (extraFiles == null || extraFiles.isEmpty()) {
            return result;
        }
        Path parent = dataFile.getParent();
        Boolean dataFileExists = null;
        for (String extra : extraFiles) {
            if (extra == null || !extra.endsWith(ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX)) {
                continue;
            }
            Path sidecar = new Path(parent, extra);
            try {
                result = result.merge(Result.of(readWithRetry(sidecar)));
            } catch (IOException e) {
                if (dataFileExists == null) {
                    dataFileExists = checkDataFileExists(dataFile);
                }
                if (!dataFileExists) {
                    LOG.debug(
                            "Ignore unreadable blobref {} because data file {} is already gone.",
                            sidecar,
                            dataFile);
                    continue;
                }
                LOG.warn(
                        "Failed to read managed BLOB reference file {}. Skip managed blob GC this run.",
                        sidecar,
                        e);
                return Result.unsafe();
            }
        }
        return result;
    }

    private boolean checkDataFileExists(Path dataFile) {
        try {
            return fileIO.exists(dataFile);
        } catch (IOException e) {
            LOG.warn(
                    "Failed to check existence of {}, treat as present for managed blob GC.",
                    dataFile,
                    e);
            return true;
        }
    }

    /**
     * Reads one sidecar. Missing, corrupt, or unsupported files are unsafe rather than thrown to
     * the caller.
     */
    public Result fromSidecar(Path sidecar) {
        try {
            List<Reference> references = readWithRetry(sidecar);
            return Result.of(references);
        } catch (IOException e) {
            LOG.warn(
                    "Failed to read managed BLOB reference file {}. Skip managed blob GC this run.",
                    sidecar,
                    e);
            return Result.unsafe();
        }
    }

    /**
     * Reads one resolved sidecar while preserving data-file-aware orphan cleanup semantics. An
     * unreadable sidecar is ignored only when its data file is already gone.
     */
    public Result fromSidecar(Path dataFile, Path sidecar) {
        try {
            return Result.of(readWithRetry(sidecar));
        } catch (IOException e) {
            if (!checkDataFileExists(dataFile)) {
                LOG.debug(
                        "Ignore unreadable blobref {} because data file {} is already gone.",
                        sidecar,
                        dataFile);
                return Result.empty();
            }
            LOG.warn(
                    "Failed to read managed BLOB reference file {}. Skip managed blob GC this run.",
                    sidecar,
                    e);
            return Result.unsafe();
        }
    }

    private List<Reference> readWithRetry(Path sidecar) throws IOException {
        IOException caught = null;
        for (int retry = 0; retry < READ_RETRY_NUM; retry++) {
            try {
                return ManagedBlobReferenceFile.read(fileIO, sidecar);
            } catch (FileNotFoundException e) {
                throw e;
            } catch (IOException e) {
                caught = e;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(READ_RETRY_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while reading " + sidecar, e);
            }
        }
        throw caught;
    }

    /** Reachability of managed BLOB packs from one or more data files. */
    public static final class Result {

        private static final Result EMPTY = new Result(Collections.<Reference>emptySet(), false);
        private static final Result UNSAFE = new Result(Collections.<Reference>emptySet(), true);

        private final Set<Reference> referenced;
        private final boolean unsafe;

        private Result(Set<Reference> referenced, boolean unsafe) {
            this.referenced = referenced;
            this.unsafe = unsafe;
        }

        public static Result empty() {
            return EMPTY;
        }

        public static Result unsafe() {
            return UNSAFE;
        }

        public static Result of(List<Reference> refs) {
            if (refs == null || refs.isEmpty()) {
                return empty();
            }
            return new Result(Collections.unmodifiableSet(new HashSet<>(refs)), false);
        }

        public Set<Reference> referenced() {
            return referenced;
        }

        public boolean isUnsafe() {
            return unsafe;
        }

        public boolean contains(Reference ref) {
            return referenced.contains(ref);
        }

        public boolean containsPackName(String fileName) {
            for (Reference reference : referenced) {
                if (reference.relativePath().equals(fileName)) {
                    return true;
                }
            }
            return false;
        }

        public Result merge(Result other) {
            if (other == null) {
                return this;
            }
            boolean mergedUnsafe = unsafe || other.unsafe;
            if (referenced.isEmpty() && other.referenced.isEmpty()) {
                return mergedUnsafe ? unsafe() : empty();
            }
            Set<Reference> refs;
            if (referenced.isEmpty()) {
                refs = other.referenced;
            } else if (other.referenced.isEmpty()) {
                refs = referenced;
            } else {
                refs = new HashSet<>(referenced);
                refs.addAll(other.referenced);
                refs = Collections.unmodifiableSet(refs);
            }
            if (mergedUnsafe == unsafe && refs == referenced) {
                return this;
            }
            if (mergedUnsafe == other.unsafe && refs == other.referenced) {
                return other;
            }
            return new Result(refs, mergedUnsafe);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Result result = (Result) o;
            return unsafe == result.unsafe && Objects.equals(referenced, result.referenced);
        }

        @Override
        public int hashCode() {
            return Objects.hash(referenced, unsafe);
        }

        @Override
        public String toString() {
            return "Result{unsafe=" + unsafe + ", referenced=" + referenced + '}';
        }
    }
}
