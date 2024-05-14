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

package org.apache.paimon.format;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.Pair;

import java.io.IOException;

/** Extracts statistics directly from file. */
public interface SimpleStatsExtractor {

    SimpleColStats[] extract(FileIO fileIO, Path path) throws IOException;

    Pair<SimpleColStats[], FileInfo> extractWithFileInfo(FileIO fileIO, Path path)
            throws IOException;

    /** File info fetched from physical file. */
    class FileInfo {

        private final long rowCount;

        public FileInfo(long rowCount) {
            this.rowCount = rowCount;
        }

        public long getRowCount() {
            return rowCount;
        }
    }
}
