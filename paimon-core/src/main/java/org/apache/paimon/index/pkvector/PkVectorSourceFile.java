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

package org.apache.paimon.index.pkvector;

import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Immutable source data-file identity captured when a vector segment is built. */
public final class PkVectorSourceFile {

    private final String fileName;
    private final long rowCount;

    public PkVectorSourceFile(String fileName, long rowCount) {
        this.fileName = Objects.requireNonNull(fileName);
        this.rowCount = rowCount;
        checkArgument(rowCount >= 0, "Source file row count must not be negative.");
    }

    public String fileName() {
        return fileName;
    }

    public long rowCount() {
        return rowCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PkVectorSourceFile that = (PkVectorSourceFile) o;
        return rowCount == that.rowCount && Objects.equals(fileName, that.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, rowCount);
    }
}
