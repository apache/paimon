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

package org.apache.paimon.tantivy;

/** Java wrapper for Tantivy index writer via JNI. Fixed schema: rowId (long) + text (String). */
public class TantivyIndexWriter implements AutoCloseable {

    static {
        NativeLoader.loadJni();
    }

    private long indexPtr;
    private boolean closed;

    public TantivyIndexWriter(String indexPath) {
        this.indexPtr = createIndex(indexPath);
        this.closed = false;
    }

    public void addDocument(long rowId, String text) {
        checkNotClosed();
        writeDocument(indexPtr, rowId, text);
    }

    public void commit() {
        checkNotClosed();
        commitIndex(indexPtr);
    }

    @Override
    public void close() {
        if (!closed) {
            freeIndex(indexPtr);
            indexPtr = 0;
            closed = true;
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("IndexWriter is already closed");
        }
    }

    // ---- native methods ----

    static native long createIndex(String indexPath);

    static native void writeDocument(long indexPtr, long rowId, String text);

    static native void commitIndex(long indexPtr);

    static native void freeIndex(long indexPtr);
}
