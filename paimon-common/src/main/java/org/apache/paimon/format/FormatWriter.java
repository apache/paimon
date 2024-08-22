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

package org.apache.paimon.format;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.BatchRecords;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/** The writer that writes records. */
public interface FormatWriter {

    /**
     * Adds an element to the encoder. The encoder may temporarily buffer the element, or
     * immediately write it to the stream.
     *
     * <p>It may be that adding this element fills up an internal buffer and causes the encoding and
     * flushing of a batch of internally buffered elements.
     *
     * @param element The element to add.
     * @throws IOException Thrown, if the element cannot be added to the encoder, or if the output
     *     stream throws an exception.
     */
    void addElement(InternalRow element) throws IOException;

    /**
     * Flushes all intermediate buffered data to the output stream. It is expected that flushing
     * often may reduce the efficiency of the encoding.
     *
     * @throws IOException Thrown if the encoder cannot be flushed, or if the output stream throws
     *     an exception.
     */
    void flush() throws IOException;

    default void writeBatch(BatchRecords batchRecords) throws IOException {
        throw new UnsupportedEncodingException("Not supported.");
    }

    /**
     * Finishes the writing. This must flush all internal buffer, finish encoding, and write
     * footers.
     *
     * <p>The writer is not expected to handle any more records via {@link #addElement(InternalRow)}
     * after this method is called.
     *
     * <p><b>Important:</b> This method MUST NOT close the stream that the writer writes to. Closing
     * the stream is expected to happen through the invoker of this method afterwards.
     *
     * @throws IOException Thrown if the finalization fails.
     */
    void finish() throws IOException;

    /**
     * Check if the writer has reached the <code>targetSize</code>.
     *
     * @param suggestedCheck Whether it needs to be checked, but subclasses can also decide whether
     *     to check it themselves.
     * @param targetSize The size of the target.
     * @return true if the target size was reached, otherwise false.
     * @throws IOException Thrown if calculating the length fails.
     */
    boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException;
}
