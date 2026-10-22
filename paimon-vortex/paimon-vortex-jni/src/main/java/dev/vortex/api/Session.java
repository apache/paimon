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

import dev.vortex.jni.NativeSession;

/**
 * A Vortex session: the native registries, the write options and the footer cache.
 *
 * <p>A session is safe to share across threads. It is expensive to create, and its footer cache
 * is only useful when it is reused, so normally one session lives for the whole process.
 */
public final class Session implements AutoCloseable {

    private long pointer;

    private Session(long pointer) {
        this.pointer = pointer;
    }

    public static Session create() {
        long ptr = NativeSession.newSession();
        if (ptr == 0) {
            throw new RuntimeException("Failed to create Vortex session");
        }
        return new Session(ptr);
    }

    /**
     * Selects the edition that writers of this session use. It replaces the selected edition of
     * the same family. Reads are not affected: a session reads files of all known editions.
     */
    public void enableEdition(String edition) {
        NativeSession.enableEdition(pointer, edition);
    }

    public long nativePointer() {
        return pointer;
    }

    @Override
    public void close() {
        if (pointer != 0) {
            NativeSession.free(pointer);
            pointer = 0;
        }
    }
}
