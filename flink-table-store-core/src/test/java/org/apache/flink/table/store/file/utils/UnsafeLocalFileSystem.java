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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileStatus;
import org.apache.flink.core.fs.local.LocalFileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/** A modified {@link LocalFileSystem} to test {@link FileUtils#safelyListFileStatus(Path)}. */
public class UnsafeLocalFileSystem extends LocalFileSystem {

    private static final Logger LOG = LoggerFactory.getLogger(UnsafeLocalFileSystem.class);

    public static final Object SHARED_LOCK = new Object();
    public static final String SCHEME = "unsafe";

    public static Path getUnsafePath(String path) {
        return new Path(SCHEME + "://" + Thread.currentThread().getName() + path);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        synchronized (SHARED_LOCK) {
            final File localf = pathToFile(f);
            FileStatus[] results;

            if (!localf.exists()) {
                return null;
            }
            if (localf.isFile()) {
                return new FileStatus[] {new LocalFileStatus(localf, this)};
            }

            final String[] names = localf.list();

            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Complete listing file status, ready for notify and wait");
                }
                SHARED_LOCK.notifyAll();
                SHARED_LOCK.wait();
            } catch (InterruptedException ignored) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Continue to get file status");
                }
            }
            if (names == null) {
                return null;
            }
            results = new FileStatus[names.length];
            for (int i = 0; i < names.length; i++) {
                results[i] = getFileStatus(new Path(f, names[i]));
            }

            return results;
        }
    }

    /** {@link FileSystemFactory} for {@link UnsafeLocalFileSystemFactory}. */
    public static final class UnsafeLocalFileSystemFactory implements FileSystemFactory {

        @Override
        public String getScheme() {
            return SCHEME;
        }

        @Override
        public FileSystem create(URI fsUri) throws IOException {
            return new UnsafeLocalFileSystem();
        }
    }
}
