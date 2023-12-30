/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.paimon.flink.lookup;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.lookup.BulkLoader;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Predicate;

/** Lookup table of full cache. */
public interface FullCacheLookupTable extends LookupTable {

    void refresh(Iterator<InternalRow> input, boolean orderByLastField) throws IOException;

    Predicate<InternalRow> recordFilter();

    byte[] toKeyBytes(InternalRow row) throws IOException;

    byte[] toValueBytes(InternalRow row) throws IOException;

    TableBulkLoader createBulkLoader();

    /** Bulk loader for the table. */
    interface TableBulkLoader {

        void write(byte[] key, byte[] value) throws BulkLoader.WriteException, IOException;

        void finish() throws IOException;
    }
}
