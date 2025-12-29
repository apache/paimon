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

import org.apache.paimon.shade.guava30.com.google.common.hash.HashCode;
import org.apache.paimon.shade.guava30.com.google.common.hash.HashFunction;
import org.apache.paimon.shade.guava30.com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** Provider for entropy inject external data paths. */
public class EntropyInjectExternalPathProvider implements ExternalPathProvider {

    private static final HashFunction HASH_FUNC = Hashing.murmur3_32();
    private static final int HASH_BINARY_STRING_BITS = 20;
    // Entropy generated will be divided into dirs with this lengths
    private static final int ENTROPY_DIR_LENGTH = 4;
    // Will create DEPTH many dirs from the entropy
    private static final int ENTROPY_DIR_DEPTH = 3;

    private final List<Path> externalTablePaths;
    private final Path relativeBucketPath;

    private int position;

    public EntropyInjectExternalPathProvider(
            List<Path> externalTablePaths, Path relativeBucketPath) {
        this.externalTablePaths = externalTablePaths;
        this.relativeBucketPath = relativeBucketPath;
    }

    @Override
    public Path getNextExternalDataPath(String fileName) {
        String hashDirs = computeHash(fileName);
        Path filePathWithHashDirs = new Path(relativeBucketPath, hashDirs + "/" + fileName);
        position++;
        if (position == externalTablePaths.size()) {
            position = 0;
        }
        return new Path(externalTablePaths.get(position), filePathWithHashDirs);
    }

    public String computeHash(String fileName) {
        HashCode hashCode = HASH_FUNC.hashString(fileName, StandardCharsets.UTF_8);
        String hashAsBinaryString = Integer.toBinaryString(hashCode.asInt() | Integer.MIN_VALUE);
        String hash =
                hashAsBinaryString.substring(hashAsBinaryString.length() - HASH_BINARY_STRING_BITS);
        return dirsFromHash(hash);
    }

    /**
     * Divides hash into directories for optimized orphan removal operation using ENTROPY_DIR_DEPTH.
     *
     * @param hash 10011001100110011001
     * @return 1001/1001/1001/10011001 with depth 3 and length 4
     */
    private String dirsFromHash(String hash) {
        StringBuilder hashWithDirs = new StringBuilder();

        for (int i = 0; i < ENTROPY_DIR_DEPTH * ENTROPY_DIR_LENGTH; i += ENTROPY_DIR_LENGTH) {
            if (i > 0) {
                hashWithDirs.append("/");
            }
            hashWithDirs.append(hash, i, Math.min(i + ENTROPY_DIR_LENGTH, hash.length()));
        }

        if (hash.length() > ENTROPY_DIR_DEPTH * ENTROPY_DIR_LENGTH) {
            hashWithDirs
                    .append("/")
                    .append(hash, ENTROPY_DIR_DEPTH * ENTROPY_DIR_LENGTH, hash.length());
        }

        return hashWithDirs.toString();
    }
}
