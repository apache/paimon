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

package dev.vortex.api;

import dev.vortex.jni.NativeDataSource;
import dev.vortex.jni.NativeScan;

import java.util.Map;
import java.util.OptionalLong;

/** A Vortex data source opened from one or more URIs. */
public final class DataSource implements AutoCloseable {

    private final Session session;
    private long pointer;

    private DataSource(Session session, long pointer) {
        this.session = session;
        this.pointer = pointer;
    }

    public static DataSource open(Session session, String uri, Map<String, String> options) {
        long ptr =
                NativeDataSource.open(
                        session.nativePointer(), new String[] {uri}, options);
        if (ptr == 0) {
            throw new RuntimeException("Failed to open Vortex data source: " + uri);
        }
        return new DataSource(session, ptr);
    }

    public long rowCount() {
        long[] out = new long[2];
        NativeDataSource.rowCount(pointer, out);
        // out[1]: 2=exact, 1=estimate, other=unknown
        if (out[1] == 2 || out[1] == 1) {
            return out[0];
        }
        return -1;
    }

    public Scan scan(ScanOptions options) {
        long projectionPtr =
                options.projection().isPresent()
                        ? options.projection().get().nativePointer()
                        : 0;
        long filterPtr =
                options.filter().isPresent() ? options.filter().get().nativePointer() : 0;
        long rangeBegin = optionalLongOrZero(options.rowRangeBegin());
        long rangeEnd = optionalLongOrZero(options.rowRangeEnd());
        long[] selectionIndices =
                options.selectionIndices().isPresent()
                        ? options.selectionIndices().get()
                        : null;
        byte selectionMode = options.selectionMode().code();
        long limit = optionalLongOrZero(options.limit());
        boolean ordered = options.ordered();

        long scanPtr =
                NativeScan.create(
                        pointer,
                        projectionPtr,
                        filterPtr,
                        rangeBegin,
                        rangeEnd,
                        selectionIndices,
                        selectionMode,
                        limit,
                        ordered);
        return Scan.fromPointer(session, scanPtr);
    }

    @Override
    public void close() {
        if (pointer != 0) {
            NativeDataSource.free(pointer);
            pointer = 0;
        }
    }

    private static long optionalLongOrZero(OptionalLong opt) {
        return opt.isPresent() ? opt.getAsLong() : 0;
    }
}
