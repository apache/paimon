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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.core.fs.FSDataOutputStream;

import java.io.IOException;

/**
 * A stream initially writes to hidden files or temp files and only creates the target file once it
 * is closed and "committed".
 */
public abstract class AtomicFsDataOutputStream extends FSDataOutputStream {

    /**
     * Closes the stream, ensuring persistence of all data (similar to {@link #sync()}). And commits
     * the file, publish (make visible) the file that the stream was writing to.
     *
     * @return Returns true if the commit may be successful. Returns false if the commit definitely
     *     fails.
     */
    public abstract boolean closeAndCommit() throws IOException;

    /**
     * Closes this stream. Closing the steam releases the local resources that the stream uses, but
     * does NOT result in durability of previously written data. This method should be interpreted
     * as a "close in order to dispose" or "close on failure".
     *
     * <p>In order to persist all previously written data, one needs to call the {@link
     * #closeAndCommit()} method.
     *
     * @throws IOException Thrown if an error occurred during closing.
     */
    @Override
    public abstract void close() throws IOException;
}
