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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Utils for local file. */
public class LocalFileUtils {

    /**
     * Get a target path(the path that replaced symbolic links with linked path) if the original
     * path contains symbolic path, return the original path otherwise.
     *
     * @param path the original path.
     * @return the path that replaced symbolic links with real path.
     */
    public static Path getTargetPathIfContainsSymbolicPath(Path path) throws IOException {
        Path targetPath = path;
        Path suffixPath = Paths.get("");
        while (path != null && path.getFileName() != null) {
            if (Files.isSymbolicLink(path)) {
                Path linkedPath = path.toRealPath();
                targetPath = Paths.get(linkedPath.toString(), suffixPath.toString());
                break;
            }
            suffixPath = Paths.get(path.getFileName().toString(), suffixPath.toString());
            path = path.getParent();
        }
        return targetPath;
    }
}
