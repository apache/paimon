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

package org.apache.paimon.fileindex;

import org.apache.paimon.fileindex.bloomfilter.BloomFilterFileIndex;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

import static org.apache.paimon.fileindex.bloomfilter.BloomFilterFileIndex.BLOOM_FILTER;

/** File index interface. To build a file index. */
public interface FileIndexer {

    FileIndexWriter createWriter();

    FileIndexReader createReader(byte[] serializedBytes);

    static FileIndexer create(String type, DataType dataType, Options options) {
        switch (type) {
            case BLOOM_FILTER:
                return new BloomFilterFileIndex(dataType, options);
            default:
                throw new RuntimeException("Doesn't support filter type: " + type);
        }
    }
}
