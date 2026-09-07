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

package org.apache.paimon.reader;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.BundleRecords;

/** A record iterator whose complete remaining logical rows can be exposed as a bundle. */
public interface BundleRecordIterator extends RecordReader.RecordIterator<InternalRow> {

    /**
     * Returns all unconsumed logical rows as bundle records.
     *
     * <p>This method must be called before the first {@link #next()} invocation. Once called,
     * {@code next()} must not be used for the same batch. The returned bundle must be consumed
     * synchronously before {@link #releaseBatch()}.
     */
    BundleRecords bundleRecords();
}
