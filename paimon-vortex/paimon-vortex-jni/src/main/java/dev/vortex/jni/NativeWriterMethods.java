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

package dev.vortex.jni;

import java.util.Map;

/** Native JNI methods for writing Vortex files. */
public final class NativeWriterMethods {

    static {
        NativeLoader.loadJni();
    }

    private NativeWriterMethods() {}

    public static native long create(String uri, long dtype, Map<String, String> options);

    public static native boolean writeBatch(long writerPtr, byte[] arrowData);

    public static native boolean writeBatchFfi(long writerPtr, long arrowArrayAddr, long arrowSchemaAddr);

    public static native void close(long writerPtr);
}
