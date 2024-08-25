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

package org.apache.paimon.io;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.InternalRow;

/**
 * Interface of bundle records. Its general implementation is Arrow Vectors. If the format is also
 * adapted to the corresponding bundle implementation, it can greatly improve the performance of
 * writing.
 *
 * @since 0.9.0
 */
@Public
public interface BundleRecords extends Iterable<InternalRow> {

    /**
     * The total row count of this batch.
     *
     * @return the number of row count.
     */
    long rowCount();
}
