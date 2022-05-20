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

package org.apache.flink.table.store.file.data;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;

import javax.annotation.concurrent.ThreadSafe;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/** Factory which produces new {@link Path}s for data files. */
@ThreadSafe
public class DataFilePathFactory {

    private final Path bucketDir;
    private final String uuid;

    private final AtomicInteger pathCount;
    private final String formatIdentifier;

    public DataFilePathFactory(Path root, String partition, int bucket, String formatIdentifier) {
        this.bucketDir = new Path(root + "/" + partition + "/bucket-" + bucket);
        this.uuid = UUID.randomUUID().toString();

        this.pathCount = new AtomicInteger(0);
        this.formatIdentifier = formatIdentifier;
    }

    public Path bucketPath() {
        return bucketDir;
    }

    public Path newPath() {
        String path =
                bucketDir
                        + "/data-"
                        + uuid
                        + "-"
                        + pathCount.getAndIncrement()
                        + "."
                        + formatIdentifier;
        return new Path(path);
    }

    public Path toPath(String fileName) {
        return new Path(bucketDir + "/" + fileName);
    }

    @VisibleForTesting
    public String uuid() {
        return uuid;
    }
}
