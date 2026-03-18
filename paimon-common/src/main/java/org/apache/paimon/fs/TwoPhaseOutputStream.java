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

/** TwoPhaseOutputStream provides a way to write to a file and get a committer that can commit. */
public abstract class TwoPhaseOutputStream extends PositionOutputStream {
    /**
     * Closes the stream for writing and returns a committer that can be used to make the written
     * data visible.
     *
     * <p>After calling this method, the stream should not be used for writing anymore. The returned
     * committer can be used to commit the data or discard it.
     *
     * @return A committer that can be used to commit the data
     * @throws IOException if an I/O error occurs during closing
     */
    public abstract Committer closeForCommit() throws IOException;

    /** A committer interface that can commit or discard the written data. */
    public interface Committer extends Serializable {

        /**
         * Commits the written data, making it visible.
         *
         * @throws IOException if an I/O error occurs during commit
         */
        void commit(FileIO fileIO) throws IOException;

        /**
         * Discards the written data, cleaning up any temporary files or resources.
         *
         * @throws IOException if an I/O error occurs during discard
         */
        void discard(FileIO fileIO) throws IOException;

        Path targetPath();

        void clean(FileIO fileIO) throws IOException;
    }
}
