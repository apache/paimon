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

import org.apache.paimon.io.BundleRecords;

import java.io.IOException;

/** Format writer with bundle interface. */
public interface BundleFormatWriter extends FormatWriter {

    /**
     * Writes a bundle of records.
     *
     * @param bundle the records to be written
     * @throws IOException if exception happens
     */
    void writeBundle(BundleRecords bundle) throws IOException;

    /**
     * Returns whether {@link #writeBundle} is equivalent to invoking {@link #addElement} for every
     * record.
     *
     * <p>An implementation returning {@code true} must preserve record values and order. It must
     * not retain borrowed buffers after {@link #writeBundle} returns unless it has copied them or
     * acquired independent ownership.
     */
    default boolean supportsRowEquivalentBundleWrite() {
        return false;
    }
}
