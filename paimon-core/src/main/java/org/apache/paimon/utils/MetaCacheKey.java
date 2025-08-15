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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;

import java.util.Objects;

/** key for meta cache at bucket level. */
public final class MetaCacheKey {
    private final Path path;
    private final BinaryRow row;
    private final int bucket;

    public MetaCacheKey(Path path, BinaryRow row, int bucket) {
        this.path = path;
        this.row = row;
        this.bucket = bucket;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MetaCacheKey)) {
            return false;
        }
        MetaCacheKey that = (MetaCacheKey) o;
        return bucket == that.bucket
                && Objects.equals(path, that.path)
                && Objects.equals(row, that.row);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, row, bucket);
    }
}
