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
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.utils.Pair;

import dev.vortex.api.VortexWriter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/** One Vortex file to write, through a native storage client or through a Paimon stream. */
final class VortexOutput {

    private final String name;
    @Nullable private final Pair<String, Map<String, String>> nativeLocation;
    @Nullable private final VortexOutputStreamWritable writable;

    private VortexOutput(
            String name,
            @Nullable Pair<String, Map<String, String>> nativeLocation,
            @Nullable VortexOutputStreamWritable writable) {
        this.name = name;
        this.nativeLocation = nativeLocation;
        this.writable = writable;
    }

    static VortexOutput of(FileIO fileIO, Path path) throws IOException {
        Pair<String, Map<String, String>> location =
                VortexUtils.nativeLocation(fileIO, path, false);
        if (location != null) {
            return new VortexOutput(path.toString(), location, null);
        }
        return new VortexOutput(
                path.toString(),
                null,
                new VortexOutputStreamWritable(fileIO.newOutputStream(path, false), true));
    }

    /** A stream that its caller keeps and closes after the writer is closed. */
    static VortexOutput of(PositionOutputStream out) {
        return new VortexOutput("stream", null, new VortexOutputStreamWritable(out, false));
    }

    String name() {
        return name;
    }

    /** The byte sink that the writer must close after the native writer is finished, if any. */
    @Nullable
    Closeable writable() {
        return writable;
    }

    VortexWriter open(Schema arrowSchema, BufferAllocator allocator) throws IOException {
        if (nativeLocation != null) {
            return VortexWriter.create(
                    VortexSessions.shared(),
                    nativeLocation.getLeft(),
                    arrowSchema,
                    allocator,
                    nativeLocation.getRight());
        }
        return VortexWriter.create(VortexSessions.shared(), writable, arrowSchema, allocator);
    }
}
