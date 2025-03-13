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

package org.apache.paimon.operation;

import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

/** The result of OrphanFilesClean. */
public class CleanOrphanFilesResult implements Serializable {
    private static final long serialVersionUID = 1L;

    public long deletedFileCount;
    public long deletedFileTotalLenInBytes;
    @Nullable public List<Path> deletedFilesPath;

    public CleanOrphanFilesResult() {}

    public CleanOrphanFilesResult(long deletedFileCount, long deletedFileTotalLenInBytes) {
        this(deletedFileCount, deletedFileTotalLenInBytes, null);
    }

    public CleanOrphanFilesResult(
            long deletedFileCount,
            long deletedFileTotalLenInBytes,
            @Nullable List<Path> deletedFilesPath) {
        this.deletedFilesPath = deletedFilesPath;
        this.deletedFileCount = deletedFileCount;
        this.deletedFileTotalLenInBytes = deletedFileTotalLenInBytes;
    }

    public long getDeletedFileCount() {
        return deletedFileCount;
    }

    public long getDeletedFileTotalLenInBytes() {
        return deletedFileTotalLenInBytes;
    }

    @Nullable
    public List<Path> getDeletedFilesPath() {
        return deletedFilesPath;
    }

    public void setDeletedFilesPath(@Nullable List<Path> deletedFilesPath) {
        this.deletedFilesPath = deletedFilesPath;
    }

    public void setDeletedFileTotalLenInBytes(long deletedFileTotalLenInBytes) {
        this.deletedFileTotalLenInBytes = deletedFileTotalLenInBytes;
    }

    public void setDeletedFileCount(long deletedFileCount) {
        this.deletedFileCount = deletedFileCount;
    }
}
