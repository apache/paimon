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

/**
 * Format writer with a row-equivalent bundle interface.
 *
 * <p>Implementations may consume a compatible bundle natively, convert or copy it, or fall back to
 * row-by-row writes. {@link #writeBundle} must preserve the same record values and order as
 * invoking {@link #addElement} for every record. It must not retain borrowed buffers after the
 * method returns unless it has copied them or acquired independent ownership.
 */
public interface BundleFormatWriter extends FormatWriter {

    /**
     * Writes a bundle with semantics equivalent to invoking {@link #addElement} for every record.
     *
     * @param bundle the records to be written
     * @throws IOException if exception happens
     */
    void writeBundle(BundleRecords bundle) throws IOException;
}
