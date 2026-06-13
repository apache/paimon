// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.paimon.index.vector;

import java.util.Map;

public final class VectorIndexWriter implements AutoCloseable {

    private final Object nativeHandleLock = new Object();
    private long nativePtr;
    private Thread nativeHandleOwner;

    public VectorIndexWriter(Map<String, String> options) {
        String[] keys = new String[options.size()];
        String[] values = new String[options.size()];
        int index = 0;
        for (Map.Entry<String, String> entry : options.entrySet()) {
            keys[index] = entry.getKey();
            values[index] = entry.getValue();
            index++;
        }
        this.nativePtr = VectorIndexNative.createWriter(keys, values);
    }

    private VectorIndexWriter(long nativePtr) {
        this.nativePtr = nativePtr;
    }

    static VectorIndexWriter fromNativePointerForTesting(long nativePtr) {
        return new VectorIndexWriter(nativePtr);
    }

    public int dimension() {
        return VectorIndexNative.writerDimension(requireOpen());
    }

    public void train(float[] data, int vectorCount) {
        if (data == null) {
            throw new NullPointerException("data");
        }
        synchronized (nativeHandleLock) {
            enterNativeHandle();
            try {
                VectorIndexNative.train(requireOpen(), data, vectorCount);
            } finally {
                exitNativeHandle();
            }
        }
    }

    public void addVectors(long[] ids, float[] data, int vectorCount) {
        if (ids == null) {
            throw new NullPointerException("ids");
        }
        if (data == null) {
            throw new NullPointerException("data");
        }
        synchronized (nativeHandleLock) {
            enterNativeHandle();
            try {
                VectorIndexNative.addVectors(requireOpen(), ids, data, vectorCount);
            } finally {
                exitNativeHandle();
            }
        }
    }

    public void writeIndex(Object output) {
        if (output == null) {
            throw new NullPointerException("output");
        }
        synchronized (nativeHandleLock) {
            enterNativeHandle();
            try {
                VectorIndexNative.writeIndex(requireOpen(), output);
            } finally {
                exitNativeHandle();
            }
        }
    }

    @Override
    public void close() {
        synchronized (nativeHandleLock) {
            enterNativeHandle();
            try {
                long ptr = nativePtr;
                nativePtr = 0L;
                if (ptr != 0L) {
                    VectorIndexNative.freeWriter(ptr);
                }
            } finally {
                exitNativeHandle();
            }
        }
    }

    private long requireOpen() {
        if (nativePtr == 0L) {
            throw new IllegalStateException("VectorIndexWriter is closed");
        }
        return nativePtr;
    }

    private void enterNativeHandle() {
        Thread current = Thread.currentThread();
        if (nativeHandleOwner == current) {
            throw new IllegalStateException("VectorIndexWriter native handle is already in use");
        }
        nativeHandleOwner = current;
    }

    private void exitNativeHandle() {
        nativeHandleOwner = null;
    }
}
