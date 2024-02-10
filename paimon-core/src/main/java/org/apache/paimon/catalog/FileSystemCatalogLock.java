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

package org.apache.paimon.catalog;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Allows users to lock table operations using file system. Only works for file system with atomic
 * operation.
 */
public class FileSystemCatalogLock implements CatalogLock {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemCatalogLock.class);
    private static final String LOCK_FILE_NAME = "lock";
    private FileIO fs;
    private final transient Path lockFile;
    private Path warehouse;

    public FileSystemCatalogLock(FileIO fileIO, Path warehouse) {
        this.fs = fileIO;
        this.warehouse = warehouse;
        this.lockFile = new Path(warehouse, "lock");
    }

    @Override
    public <T> T runWithLock(String database, String table, Callable<T> callable) throws Exception {
        try {
            synchronized (LOCK_FILE_NAME) {
                while (!tryLock()) {
                    Thread.sleep(100);
                }
                return callable.call();
            }
        } finally {
            unlock();
        }
    }

    public boolean tryLock() {
        try {
            synchronized (LOCK_FILE_NAME) {
                if (fs.exists(this.lockFile)) {
                    fs.delete(this.lockFile, true);
                }
                acquireLock();
                return fs.exists(this.lockFile);
            }
        } catch (IOException e) {
            LOG.info(generateLogInfo(LockState.FAILED_TO_ACQUIRE), e);
            return false;
        }
    }

    public void unlock() {
        synchronized (LOCK_FILE_NAME) {
            try {
                if (fs.exists(this.lockFile)) {
                    fs.delete(this.lockFile, true);
                }
            } catch (IOException io) {
                throw new RuntimeException(generateLogInfo(LockState.FAILED_TO_RELEASE), io);
            }
        }
    }

    private void acquireLock() {
        try {
            synchronized (LOCK_FILE_NAME) {
                fs.newOutputStream(this.lockFile, false).close();
            }
        } catch (IOException e) {
            throw new RuntimeException(generateLogInfo(LockState.FAILED_TO_ACQUIRE), e);
        }
    }

    public String getLock() {
        return this.lockFile.toString();
    }

    protected String generateLogInfo(LockState state) {
        return String.format("%s lock at: %s", state.name(), getLock());
    }

    public static CatalogLock.Factory createFactory(FileIO fileIO, Path warehouse) {
        return new FileSystemCatalogLockFactory(fileIO, warehouse);
    }

    public void close() throws IOException {
        synchronized (LOCK_FILE_NAME) {
            try {
                fs.delete(this.lockFile, true);
            } catch (IOException e) {
                throw new RuntimeException(generateLogInfo(LockState.FAILED_TO_RELEASE), e);
            }
        }
    }

    private static class FileSystemCatalogLockFactory implements CatalogLock.Factory {

        private static final long serialVersionUID = 1L;

        private final FileIO fileIO;
        private final Path warehouse;

        public FileSystemCatalogLockFactory(FileIO fileIO, Path warehouse) {
            this.fileIO = fileIO;
            this.warehouse = warehouse;
        }

        @Override
        public CatalogLock create() {
            return new FileSystemCatalogLock(fileIO, warehouse);
        }
    }
}
