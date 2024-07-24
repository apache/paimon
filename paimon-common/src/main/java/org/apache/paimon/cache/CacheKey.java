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

package org.apache.paimon.cache;

import java.util.Objects;

/** The cache key. */
public class CacheKey {

    private final String fileName;
    private final long offset;

    public CacheKey(String fileName, long offset) {
        this.fileName = fileName;
        this.offset = offset;
    }

    public String getFileName() {
        return fileName;
    }

    public long getOffset() {
        return offset;
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
        return Objects.equals(fileName, cacheKey.fileName) && offset == cacheKey.offset;
    }

    @Override
    public String toString() {
        return "CacheKey{" + "file='" + fileName + '\'' + ", offset=" + offset + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, offset);
    }
}
