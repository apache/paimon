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

package org.apache.paimon.fs;

import java.io.IOException;
import java.io.Serializable;

/**
 * An output stream that stages data and produces a committer to publish it.
 *
 * <p>Staged data is not published at the target before commit is invoked. A successful commit makes
 * the complete data visible but does not imply atomic replacement. If commit fails, the target
 * state is unspecified.
 */
public abstract class TwoPhaseOutputStream extends PositionOutputStream {

    /**
     * Closes the stream for writing and returns a committer for the staged data.
     *
     * <p>After this call, the stream must not be used for writing. The staged data remains
     * unpublished until {@link Committer#commit(FileIO)} is invoked.
     *
     * @return a committer that can publish or discard the staged data
     * @throws IOException if an I/O error occurs during closing
     */
    public abstract Committer closeForCommit() throws IOException;

    /** A serializable handle that can publish or discard one stream's staged data. */
    public interface Committer extends Serializable {

        /**
         * Publishes the complete staged data at {@link #targetPath()}.
         *
         * <p>A successful return makes the complete data visible but does not guarantee atomic
         * replacement. If this method throws, the target state is unspecified.
         *
         * @throws IOException if an I/O error occurs during commit
         */
        void commit(FileIO fileIO) throws IOException;

        /**
         * Discards this write's staged data instead of publishing it.
         *
         * <p>Only resources created by this write may be removed. In particular, discard must not
         * remove target content or resources created by another writer.
         *
         * @throws IOException if an I/O error occurs during discard
         */
        void discard(FileIO fileIO) throws IOException;

        /** Returns the path where a successful commit publishes the staged data. */
        Path targetPath();

        /**
         * Releases staging resources that this write no longer needs after {@link #commit}
         * succeeds.
         *
         * <p>This method may do nothing. It may remove only resources created by this write and
         * must not remove the committed target or resources owned by another writer.
         *
         * @throws IOException if an I/O error occurs during cleaning
         */
        void clean(FileIO fileIO) throws IOException;
    }
}
