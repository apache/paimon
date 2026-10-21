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

package org.apache.paimon.format.vortex;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.Pair;

import dev.vortex.api.DataSource;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.Map;

/** One Vortex file to read, through a native storage client or through a Paimon FileIO. */
final class VortexInput {

    @Nullable private final Pair<String, Map<String, String>> nativeLocation;
    @Nullable private final VortexFileIOReadable readable;

    private VortexInput(
            @Nullable Pair<String, Map<String, String>> nativeLocation,
            @Nullable VortexFileIOReadable readable) {
        this.nativeLocation = nativeLocation;
        this.readable = readable;
    }

    static VortexInput of(FileIO fileIO, Path path, long fileSize) {
        Pair<String, Map<String, String>> location = VortexUtils.nativeLocation(fileIO, path, true);
        if (location != null) {
            return new VortexInput(location, null);
        }
        return new VortexInput(null, new VortexFileIOReadable(fileIO, path, fileSize));
    }

    /** The byte source that the reader must close after the data source, if any. */
    @Nullable
    Closeable readable() {
        return readable;
    }

    DataSource open() {
        if (nativeLocation != null) {
            return DataSource.open(
                    VortexSessions.shared(), nativeLocation.getLeft(), nativeLocation.getRight());
        }
        return DataSource.open(VortexSessions.shared(), readable, 0);
    }
}
