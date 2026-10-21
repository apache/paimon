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

import dev.vortex.api.Session;
import dev.vortex.jni.NativeRuntime;

/**
 * The process-wide Vortex session and native worker pool.
 *
 * <p>A session holds the native registries and the footer cache. It is safe to share across threads
 * and expensive to create, so all readers and writers use one session that lives until the JVM
 * exits.
 */
final class VortexSessions {

    private static volatile int workerThreads;

    private VortexSessions() {}

    static Session shared() {
        return Holder.SHARED;
    }

    /**
     * Grows the JVM-wide native worker pool to at least {@code count} threads. Without workers,
     * only the Java thread that waits on a scan or a write drives its decode or encode work. The
     * pool is shared by every table in the process, so it never shrinks here.
     */
    static void ensureWorkerThreads(int count) {
        if (count <= workerThreads) {
            return;
        }

        synchronized (VortexSessions.class) {
            if (count > workerThreads) {
                NativeRuntime.setWorkerThreads(count);
                workerThreads = count;
            }
        }
    }

    private static final class Holder {
        private static final Session SHARED = Session.create();
    }
}
