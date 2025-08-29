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

import org.apache.paimon.fs.Path;

/**
 * Utility class for resolving compacted changelog file paths.
 *
 * <p>This class provides functionality to resolve fake compacted changelog file paths to their real
 * file paths, following the same protocol as CompactedChangelogFormatReaderFactory.
 */
public class CompactedChangelogPathResolver {

    /**
     * Resolves compacted changelog file path to its real file path. Handles both real and fake
     * compacted changelog file names as described in CompactedChangelogFormatReaderFactory.
     *
     * @param path the file path to resolve
     * @return the resolved real file path
     */
    public static Path resolveCompactedChangelogPath(Path path) {
        // Check if this is a compacted changelog file by its .cc- extension
        String[] nameAndFormat = path.getName().split("\\.");
        if (nameAndFormat.length < 2 || !nameAndFormat[1].startsWith("cc-")) {
            return path;
        }

        String[] names = nameAndFormat[0].split("\\$");
        String[] split = names[1].split("-");
        if (split.length == 2) {
            // Real file: compacted-changelog-xxx$bid-len.cc-format
            return path;
        } else {
            // Fake file: compacted-changelog-xxx$bid-len-off-len2.cc-format
            // Resolve to real file: bucket-bid/compacted-changelog-xxx$bid-len.cc-format
            return new Path(
                    path.getParent().getParent(),
                    "bucket-"
                            + split[0]
                            + "/"
                            + names[0]
                            + "$"
                            + split[0]
                            + "-"
                            + split[1]
                            + "."
                            + nameAndFormat[1]);
        }
    }
}
