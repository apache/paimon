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

/** Java wrapper for Tantivy index searcher via JNI. Returns (rowId, score) pairs. */
public class TantivySearcher implements AutoCloseable {

    static {
        NativeLoader.loadJni();
    }

    private long searcherPtr;
    private boolean closed;

    /** Open a searcher from a local index directory. */
    public TantivySearcher(String indexPath) {
        this.searcherPtr = openIndex(indexPath);
        this.closed = false;
    }

    /**
     * Open a searcher from a stream-backed archive.
     *
     * @param fileNames names of files in the archive
     * @param fileOffsets byte offset of each file in the stream
     * @param fileLengths byte length of each file
     * @param streamInput callback for seek/read operations on the stream
     */
    public TantivySearcher(
            String[] fileNames,
            long[] fileOffsets,
            long[] fileLengths,
            StreamFileInput streamInput) {
        this.searcherPtr = openFromStream(fileNames, fileOffsets, fileLengths, streamInput);
        this.closed = false;
    }

    /**
     * Search the index with a query string, returning top N results ranked by score.
     *
     * @param queryString the query text
     * @param limit max number of results
     * @return search results containing rowIds and scores
     */
    public SearchResult search(String queryString, int limit) {
        checkNotClosed();
        return searchIndex(searcherPtr, queryString, limit);
    }

    @Override
    public void close() {
        if (!closed) {
            freeSearcher(searcherPtr);
            searcherPtr = 0;
            closed = true;
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Searcher is already closed");
        }
    }

    // ---- native methods ----

    static native long openIndex(String indexPath);

    static native long openFromStream(
            String[] fileNames, long[] fileOffsets, long[] fileLengths, StreamFileInput streamInput);

    static native SearchResult searchIndex(long searcherPtr, String queryString, int limit);

    static native void freeSearcher(long searcherPtr);
}
