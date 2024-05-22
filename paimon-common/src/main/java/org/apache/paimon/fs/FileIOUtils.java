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

import org.apache.paimon.catalog.CatalogContext;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

import static org.apache.paimon.utils.ThreadUtils.newDaemonThreadFactory;

/** Utils for {@link FileIO}. */
public class FileIOUtils {

    public static final ExecutorService IO_THREAD_POOL =
            Executors.newCachedThreadPool(newDaemonThreadFactory("IO-THREAD-POOL"));

    private static volatile ForkJoinPool scanIoForkJoinPool;

    public static FileIOLoader checkAccess(FileIOLoader fileIO, Path path, CatalogContext config)
            throws IOException {
        if (fileIO == null) {
            return null;
        }

        // check access
        FileIO io = fileIO.load(path);
        io.configure(config);
        io.exists(path);
        return fileIO;
    }
}
