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

package org.apache.paimon.lucene.index;

import org.apache.lucene.store.MMapDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

/** A wrapper of MMapDirectory for vector index. */
public class IndexMMapDirectory implements AutoCloseable {
    private final Path path;
    private final MMapDirectory mmapDirectory;

    public IndexMMapDirectory() throws IOException {
        this.path = java.nio.file.Files.createTempDirectory("paimon-lucene-" + UUID.randomUUID());
        this.mmapDirectory = new MMapDirectory(path);
    }

    public MMapDirectory directory() {
        return mmapDirectory;
    }

    public void close() throws Exception {
        mmapDirectory.close();
        if (java.nio.file.Files.exists(path)) {
            java.nio.file.Files.walk(path)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(
                            p -> {
                                try {
                                    java.nio.file.Files.delete(p);
                                } catch (IOException e) {
                                    // Ignore cleanup errors
                                }
                            });
        }
    }
}
