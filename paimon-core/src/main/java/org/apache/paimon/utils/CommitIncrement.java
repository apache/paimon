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

import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;

import javax.annotation.Nullable;

/** Changes to commit. */
public class CommitIncrement {

    private final DataIncrement dataIncrement;
    private final CompactIncrement compactIncrement;
    @Nullable private final CompactDeletionFile compactDeletionFile;

    public CommitIncrement(
            DataIncrement dataIncrement,
            CompactIncrement compactIncrement,
            @Nullable CompactDeletionFile compactDeletionFile) {
        this.dataIncrement = dataIncrement;
        this.compactIncrement = compactIncrement;
        this.compactDeletionFile = compactDeletionFile;
    }

    public DataIncrement newFilesIncrement() {
        return dataIncrement;
    }

    public CompactIncrement compactIncrement() {
        return compactIncrement;
    }

    @Nullable
    public CompactDeletionFile compactDeletionFile() {
        return compactDeletionFile;
    }

    @Override
    public String toString() {
        return dataIncrement.toString() + "\n" + compactIncrement + "\n" + compactDeletionFile;
    }
}
