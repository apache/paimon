/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/** A modified {@link LocalFileSystem} supporting atomic rename. */
public class TestAtomicRenameFileSystem extends LocalFileSystem {

    public static final String SCHEME = "test";

    // the same file system object is cached and shared in the same JVM,
    // so we can use java locks to ensure atomic renaming
    private final ReentrantLock renameLock;

    public TestAtomicRenameFileSystem() {
        this.renameLock = new ReentrantLock();
    }

    @Override
    public boolean rename(final Path src, final Path dst) throws IOException {
        File srcFile = pathToFile(src);
        File dstFile = pathToFile(dst);
        File dstParent = dstFile.getParentFile();
        dstParent.mkdirs();
        try {
            renameLock.lock();
            if (dstFile.exists()) {
                return false;
            }
            Files.move(srcFile.toPath(), dstFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
            return true;
        } catch (NoSuchFileException
                | AccessDeniedException
                | DirectoryNotEmptyException
                | SecurityException e) {
            return false;
        } finally {
            renameLock.unlock();
        }
    }

    @Override
    public FileStatus[] listStatus(final Path f) throws IOException {
        // TODO remove this method once FLINK-25453 is fixed
        File localf = pathToFile(f);
        if (!localf.exists()) {
            return null;
        }
        if (localf.isFile()) {
            return new FileStatus[] {new LocalFileStatus(localf, this)};
        }

        final String[] names = localf.list();
        if (names == null) {
            return null;
        }
        List<FileStatus> results = new ArrayList<>();
        for (String name : names) {
            try {
                results.add(getFileStatus(new Path(f, name)));
            } catch (FileNotFoundException e) {
                // ignore the files not found since the dir list may have have changed
                // since the names[] list was generated.
            }
        }

        return results.toArray(new FileStatus[0]);
    }

    @Override
    public URI getUri() {
        return URI.create(SCHEME + ":///");
    }

    /** {@link FileSystemFactory} for {@link TestAtomicRenameFileSystem}. */
    public static final class TestAtomicRenameFileSystemFactory implements FileSystemFactory {

        @Override
        public String getScheme() {
            return SCHEME;
        }

        @Override
        public FileSystem create(URI uri) throws IOException {
            return new TraceableFileSystem(new TestAtomicRenameFileSystem());
        }
    }
}
