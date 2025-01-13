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

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/** Provider for external data paths. */
public class ExternalPathProvider implements Serializable {

    private final List<Path> externalTablePaths;
    private final Path relativeBucketPath;

    private int position;

    public ExternalPathProvider(List<Path> externalTablePaths, Path relativeBucketPath) {
        this.externalTablePaths = externalTablePaths;
        this.relativeBucketPath = relativeBucketPath;
        this.position = ThreadLocalRandom.current().nextInt(externalTablePaths.size());
    }

    /**
     * Get the next external data path.
     *
     * @return the next external data path
     */
    public Path getNextExternalDataPath(String fileName) {
        position++;
        if (position == externalTablePaths.size()) {
            position = 0;
        }
        return new Path(new Path(externalTablePaths.get(position), relativeBucketPath), fileName);
    }
}
