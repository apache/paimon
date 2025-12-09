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

package org.apache.paimon.io.cache;

import org.apache.paimon.fs.Path;

import java.io.RandomAccessFile;
import java.util.Objects;

/** Key for cache manager. */
public interface CacheKey {

    static PositionCacheKey forPosition(Path filePath, long position, int length, boolean isIndex) {
        return new PositionCacheKey(filePath, position, length, isIndex);
    }

    static PageIndexCacheKey forPageIndex(RandomAccessFile file, int pageSize, int pageIndex) {
        return new PageIndexCacheKey(file, pageSize, pageIndex, false);
    }

    /** @return Whether this cache key is for index cache. */
    boolean isIndex();

    /** Key for file position of a file path (could be remote) and length. */
    class PositionCacheKey implements CacheKey {

        private final Path filePath;
        private final long position;
        private final int length;
        private final boolean isIndex;

        private PositionCacheKey(Path filePath, long position, int length, boolean isIndex) {
            this.filePath = filePath;
            this.position = position;
            this.length = length;
            this.isIndex = isIndex;
        }

        public long position() {
            return position;
        }

        public int length() {
            return length;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PositionCacheKey that = (PositionCacheKey) o;
            return position == that.position
                    && length == that.length
                    && isIndex == that.isIndex
                    && Objects.equals(filePath, that.filePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filePath, position, length, isIndex);
        }

        @Override
        public boolean isIndex() {
            return isIndex;
        }
    }

    /** Key for file page index. */
    class PageIndexCacheKey implements CacheKey {

        private final RandomAccessFile file;
        private final int pageSize;
        private final int pageIndex;
        private final boolean isIndex;

        private PageIndexCacheKey(
                RandomAccessFile file, int pageSize, int pageIndex, boolean isIndex) {
            this.file = file;
            this.pageSize = pageSize;
            this.pageIndex = pageIndex;
            this.isIndex = isIndex;
        }

        public int pageIndex() {
            return pageIndex;
        }

        @Override
        public boolean isIndex() {
            return isIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PageIndexCacheKey that = (PageIndexCacheKey) o;
            return pageSize == that.pageSize
                    && pageIndex == that.pageIndex
                    && isIndex == that.isIndex
                    && Objects.equals(file, that.file);
        }

        @Override
        public int hashCode() {
            return Objects.hash(file, pageSize, pageIndex, isIndex);
        }
    }
}
