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

/** Native methods for Vortex scan operations. */
public final class NativeScan {

    static {
        NativeLoader.loadJni();
    }

    private NativeScan() {}

    public static native long create(
            long dataSourcePtr,
            long projectionExprPtr,
            long filterExprPtr,
            long rowRangeBegin,
            long rowRangeEnd,
            long[] selectionIndices,
            byte selectionModeCode,
            long limit,
            boolean ordered);

    public static native void free(long scanPtr);

    public static native void arrowSchema(long scanPtr, long arrowSchemaOutAddr);

    public static native void partitionCount(long scanPtr, long[] resultOut);

    /** Returns the next partition pointer, or 0 when exhausted. */
    public static native long nextPartition(long scanPtr);
}
