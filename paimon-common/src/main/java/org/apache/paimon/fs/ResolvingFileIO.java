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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.paimon.options.CatalogOptions.RESOLVING_FILE_IO_ENABLED;

/**
 * An implementation of {@link FileIO} that supports multiple file system schemas. It dynamically
 * selects the appropriate {@link FileIO} based on the URI scheme of the given path.
 */
public class ResolvingFileIO implements FileIO {

    private static final long serialVersionUID = 1L;

    private final Map<CacheKey, FileIO> fileIOMap = new ConcurrentHashMap<>();

    private CatalogContext context;

    // TODO, how to decide the real fileio is object store or not?
    @Override
    public boolean isObjectStore() {
        String warehouse = context.options().get(CatalogOptions.WAREHOUSE);
        if (warehouse == null) {
            return false;
        }
        Path path = new Path(warehouse);
        String scheme = path.toUri().getScheme();
        return scheme != null
                && !scheme.equalsIgnoreCase("file")
                && !scheme.equalsIgnoreCase("hdfs");
    }

    @Override
    public void configure(CatalogContext context) {
        Options options = new Options();
        context.options().toMap().forEach(options::set);
        options.set(RESOLVING_FILE_IO_ENABLED, false);
        this.context = context.copy(options, context.preferIO(), context.fallbackIO());
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        return wrap(() -> fileIO(path).newInputStream(path));
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        return wrap(() -> fileIO(path).newOutputStream(path, overwrite));
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return wrap(() -> fileIO(path).getFileStatus(path));
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return wrap(() -> fileIO(path).listStatus(path));
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return wrap(() -> fileIO(path).exists(path));
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return wrap(() -> fileIO(path).delete(path, recursive));
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return wrap(() -> fileIO(path).mkdirs(path));
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return wrap(() -> fileIO(src).rename(src, dst));
    }

    @VisibleForTesting
    public FileIO fileIO(Path path) throws IOException {
        CacheKey cacheKey = new CacheKey(path.toUri().getScheme(), path.toUri().getAuthority());
        return fileIOMap.computeIfAbsent(
                cacheKey,
                k -> {
                    try {
                        return FileIO.get(path, context);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private <T> T wrap(Func<T> func) throws IOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(ResolvingFileIO.class.getClassLoader());
            return func.apply();
        } finally {
            Thread.currentThread().setContextClassLoader(cl);
        }
    }

    /** Apply function with wrapping classloader. */
    @FunctionalInterface
    protected interface Func<T> {
        T apply() throws IOException;
    }

    private static class CacheKey implements Serializable {
        private final String scheme;
        private final String authority;

        private CacheKey(String scheme, String authority) {
            this.scheme = scheme;
            this.authority = authority;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(scheme, cacheKey.scheme)
                    && Objects.equals(authority, cacheKey.authority);
        }

        @Override
        public int hashCode() {
            return Objects.hash(scheme, authority);
        }
    }
}
