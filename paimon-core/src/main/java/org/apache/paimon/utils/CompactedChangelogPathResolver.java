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
 * file paths.
 *
 * <p><b>File Name Protocol</b>
 *
 * <p>There are two kinds of file name. In the following description, <code>bid1</code> and <code>
 * bid2</code> are bucket id, <code>off</code> is offset, <code>len1</code> and <code>len2</code>
 * are lengths.
 *
 * <ul>
 *   <li><code>bucket-bid1/compacted-changelog-xxx$bid1-len1</code>: This is the real file name. If
 *       this file name is recorded in manifest file meta, reader should read the bytes of this file
 *       starting from offset <code>0</code> with length <code>len1</code>.
 *   <li><code>bucket-bid2/compacted-changelog-xxx$bid1-len1-off-len2</code>: This is the fake file
 *       name. Reader should read the bytes of file <code>
 *       bucket-bid1/compacted-changelog-xxx$bid1-len1</code> starting from offset <code>off</code>
 *       with length <code>len2</code>.
 * </ul>
 */
public class CompactedChangelogPathResolver {

    /**
     * Checks if the given path is a compacted changelog file path.
     *
     * @param path the file path to check
     * @return true if the path is a compacted changelog file, false otherwise
     */
    public static boolean isCompactedChangelogPath(Path path) {
        return path.getName().startsWith("compacted-changelog-");
    }

    /**
     * Resolves a file path, handling compacted changelog file path resolution if applicable.
     *
     * <p>For compacted changelog files, resolves fake file paths to their real file paths as
     * described in the protocol above. For non-compacted changelog files, returns the path
     * unchanged.
     *
     * @param path the file path to resolve
     * @return the resolved real file path for compacted changelog files, or the original path
     *     unchanged for other files
     */
    public static Path resolveCompactedChangelogPath(Path path) {
        if (!isCompactedChangelogPath(path)) {
            return path;
        }
        return decodePath(path).getPath();
    }

    /**
     * Decodes a compacted changelog file path to extract the real path, offset, and length.
     *
     * @param path the file path to decode
     * @return the decode result containing real path, offset, and length
     */
    public static DecodeResult decodePath(Path path) {
        String[] nameAndFormat = path.getName().split("\\.");
        String[] names = nameAndFormat[0].split("\\$");
        String[] split = names[1].split("-");
        if (split.length == 2) {
            return new DecodeResult(path, 0, Long.parseLong(split[1]));
        } else {
            Path realPath =
                    new Path(
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
            return new DecodeResult(realPath, Long.parseLong(split[2]), Long.parseLong(split[3]));
        }
    }

    /** Result of decoding a compacted changelog file path. */
    public static class DecodeResult {

        private final Path path;
        private final long offset;
        private final long length;

        public DecodeResult(Path path, long offset, long length) {
            this.path = path;
            this.offset = offset;
            this.length = length;
        }

        public Path getPath() {
            return path;
        }

        public long getOffset() {
            return offset;
        }

        public long getLength() {
            return length;
        }
    }
}
