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
import java.util.Arrays;
import java.util.concurrent.Callable;

/** FileSystemCatalogLock. */
public class FileSystemCatalogLock implements CatalogLock {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemCatalogLock.class);
    private static final String LOCK_FILE_NAME = "lock";
    private static final Object lockObject = new Object();
    private FileIO fs;
    private final transient Path lockFile;
    private Path warehouse;
    private final long checkMaxSleep;
    private final long acquireTimeout;

    private final RetryHelper<Boolean, RuntimeException> lockRetryHelper;

    public FileSystemCatalogLock(
            FileIO fileIO, Path warehouse, long checkMaxSleep, long acquireTimeout) {
        this.fs = fileIO;
        this.warehouse = warehouse;
        this.lockFile = new Path(warehouse, "lock");
        this.checkMaxSleep = checkMaxSleep;
        this.acquireTimeout = acquireTimeout;
        lockRetryHelper =
                new RetryHelper<>(
                        1000,
                        15,
                        1000,
                        Arrays.asList(RuntimeException.class, InterruptedException.class),
                        "acquire lock");
    }

    //    private void lock() {
    //        int cnt =15;
    //        while()
    //        while (true) {
    //            if (tryLock()) {
    //                break;
    //            }
    //        }
    //    }

    //    public void lock() {
    //        //        lockRetryHelper.start(() -> {
    //        //            try {
    //        //                if (!tryLock()) {
    //        //                    throw new RuntimeException("Unable to acquire the lock. Current
    // lock
    //        // owner information : ");
    //        //                }
    //        //                return true;
    //        //            } catch (Exception e) {
    //        //                throw new RuntimeException(e);
    //        //            }
    //        //        });
    //        int retryCount = 0;
    //        boolean acquired = false;
    //
    //        while (retryCount <= 15) {
    //            try {
    //                acquired = tryLock();
    //                if (acquired) {
    //                    break;
    //                }
    //                LOG.info("Retrying to acquire lock...");
    //                Thread.sleep(1000);
    //                retryCount++;
    //            } catch (InterruptedException e) {
    //                if (retryCount >= 15) {
    //                    throw new RuntimeException("Unable to acquire lock, lock object ", e);
    //                }
    //            }
    //        }
    //    }

    public boolean tryLock() {
        try {
            synchronized (LOCK_FILE_NAME) {
                // synchronized (lockObject) {
                // Check whether lock is already expired, if so try to delete lock file
                // if (fs.exists(this.lockFile) && checkIfExpired()) {
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
            // synchronized (lockObject) {
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
            // synchronized (lockObject) {
            synchronized (LOCK_FILE_NAME) {
                // fs.writeFileUtf8(this.lockFile, "");
                fs.newOutputStream(this.lockFile, false).close();
            }
            // fs.create(this.lockFile, false).close();
        } catch (IOException e) {
            throw new RuntimeException(generateLogInfo(LockState.FAILED_TO_ACQUIRE), e);
        }
    }
    //    public void close() throws IOException {
    //
    //    }

    //    private boolean checkIfExpired() {
    //        if (lockTimeoutMinutes == 0) {
    //            return false;
    //        }
    //        try {
    //            long modificationTime = fs.getFileStatus(this.lockFile).getModificationTime();
    //            if (System.currentTimeMillis() - modificationTime > lockTimeoutMinutes * 60 *
    // 1000L) {
    //                return true;
    //            }
    //        } catch (IOException | HoodieIOException e) {
    //            LOG.error(generateLogStatement(LockState.ALREADY_RELEASED) + " failed to get
    // lockFile's modification time", e);
    //        }
    //        return false;
    //    }

    public String getLock() {
        return this.lockFile.toString();
    }

    protected String generateLogInfo(LockState state) {
        return String.format("%s lock at: %s", state.name(), getLock());
    }

    public static CatalogLock.Factory createFactory(FileIO fileIO, Path warehouse) {
        return new FileSystemCatalogLockFactory(fileIO, warehouse);
    }

    @Override
    public <T> T runWithLock(String database, String table, Callable<T> callable) throws Exception {
        synchronized (LOCK_FILE_NAME) {
            try {
                while (!tryLock()) {
                    Thread.sleep(1000);
                }
                return callable.call();
            } finally {
                unlock();
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (LOCK_FILE_NAME) {
            // synchronized (lockObject) {
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
            return new FileSystemCatalogLock(fileIO, warehouse, 1000, 1000);
        }
    }
}
