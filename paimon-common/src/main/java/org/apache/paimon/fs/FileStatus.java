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

import org.apache.paimon.annotation.Public;

import javax.annotation.Nullable;

/**
 * A snapshot of provider-neutral metadata for a file or logical directory.
 *
 * <p>The values do not change when the underlying path changes. Owner and access time, as well as
 * directory length and modification time, are not portable across file systems.
 *
 * @since 0.4.0
 */
@Public
public interface FileStatus {

    /**
     * Returns the file length in bytes. The value for a directory is not portable.
     *
     * @return the file length in bytes
     */
    long getLen();

    /**
     * Checks if this object represents a directory.
     *
     * @return <code>true</code> if this is a directory, <code>false</code> otherwise
     */
    boolean isDir();

    /**
     * Returns the corresponding Path to the FileStatus.
     *
     * @return the corresponding Path to the FileStatus
     */
    Path getPath();

    /**
     * Returns the last modification time in milliseconds since the epoch. The value for a directory
     * is not portable.
     *
     * @return the last modification time
     */
    long getModificationTime();

    /**
     * Returns the last access time in milliseconds since the epoch, if available. This value is not
     * portable and may be zero.
     *
     * @return the last access time, or zero when unavailable
     */
    default long getAccessTime() {
        return 0;
    }

    /**
     * Returns the owner, if available. This value is not portable.
     *
     * @return the owner, or null when unavailable
     */
    @Nullable
    default String getOwner() {
        return null;
    }
}
