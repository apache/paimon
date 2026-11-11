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

import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.IOFunction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Format specific file metadata, such as a Parquet footer, shared by the readers of the same files
 * so that the metadata is read only once. The metadata of a file is cached by its path, so all
 * users of a cache must read the same kind of metadata of a file with the same options. Not thread
 * safe.
 */
public class FileMetadataCache {

    private final Map<Path, Object> metadata = new HashMap<>();

    @SuppressWarnings("unchecked")
    public <T> T getOrLoad(Path path, IOFunction<Path, T> loader) throws IOException {
        Object value = metadata.get(path);
        if (value == null) {
            value = loader.apply(path);
            metadata.put(path, value);
        }
        return (T) value;
    }
}
