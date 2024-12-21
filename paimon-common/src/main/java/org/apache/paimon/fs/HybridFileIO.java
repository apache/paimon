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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A hybrid implementation of {@link FileIO} that supports multiple file system schemas. It
 * dynamically selects the appropriate {@link FileIO} based on the URI scheme of the given path.
 */
public class HybridFileIO implements FileIO {

    private static final long serialVersionUID = 1L;

    protected Options options;

    private Map<String, FileIO> fileIOMap;
    private volatile FileIO fallbackFileIO;

    @Override
    public boolean isObjectStore() {
        if (options.get(CoreOptions.DATA_FILE_EXTERNAL_PATH) != null
                && ((options.get(CoreOptions.DATA_FILE_EXTERNAL_PATH).startsWith("oss://")
                        || (options.get(CoreOptions.DATA_FILE_EXTERNAL_PATH)
                                .startsWith("s3://"))))) {
            return true;
        }
        return false;
    }

    @Override
    public void configure(CatalogContext context) {
        this.options = context.options();
        this.fileIOMap = new ConcurrentHashMap<>();
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

    private FileIO fileIO(Path path) throws IOException {
        String schema = path.toUri().getScheme();
        if (schema == null) {
            if (fallbackFileIO == null) {
                synchronized (this) {
                    if (fallbackFileIO == null) {
                        CatalogContext catalogContext = CatalogContext.create(options);
                        fallbackFileIO = FileIO.get(path, catalogContext);
                    }
                }
            }
            return fallbackFileIO;
        }

        if (!fileIOMap.containsKey(schema)) {
            synchronized (this) {
                if (!fileIOMap.containsKey(schema)) {
                    CatalogContext catalogContext = CatalogContext.create(options);
                    FileIO fileIO = FileIO.get(path, catalogContext);
                    fileIOMap.put(path.toUri().getScheme(), fileIO);
                }
            }
        }
        return fileIOMap.get(path.toUri().getScheme());
    }

    private <T> T wrap(Func<T> func) throws IOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(HybridFileIO.class.getClassLoader());
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
}
