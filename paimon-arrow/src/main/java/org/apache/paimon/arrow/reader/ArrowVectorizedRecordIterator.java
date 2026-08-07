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

package org.apache.paimon.arrow.reader;

import org.apache.paimon.arrow.ArrowBundleRecords;
import org.apache.paimon.reader.VectorizedRecordIterator;

/** A {@link VectorizedRecordIterator} which can expose its Arrow batch for direct bundle writes. */
public interface ArrowVectorizedRecordIterator extends VectorizedRecordIterator {

    /**
     * Returns a borrowed view of the Arrow vectors backing {@link #batch()}.
     *
     * <p>The caller does not own the batch and must not retain or close it. Its row order and count
     * correspond to {@link #batch()}, and it is valid only until {@link #releaseBatch()}.
     */
    ArrowBundleRecords arrowBundle();
}
