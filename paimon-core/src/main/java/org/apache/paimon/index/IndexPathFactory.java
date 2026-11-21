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

package org.apache.paimon.index;

import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

/** Path factory to create an index path. */
public interface IndexPathFactory {

    /** Generates a file name and allocates a path for a new index file. */
    Path newPath();

    /** Allocates a path for a new index file with the given file name. */
    Path newPathFromName(String fileName);

    /** Resolves an existing file without allocating an external location. */
    Path toPath(String fileName, @Nullable String externalPath);

    default Path toPath(IndexFileMeta file) {
        return toPath(file.fileName(), file.externalPath());
    }

    boolean isExternalPath();
}
