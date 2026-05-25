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

package org.apache.paimon.globalindex.testfulltext;

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.VarCharType;

import java.io.IOException;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A test-only {@link GlobalIndexer} for full-text search. Uses brute-force in-memory inverted index
 * for text queries. No native library dependency required.
 */
public class TestFullTextGlobalIndexer implements GlobalIndexer {

    private final DataType fieldType;

    public TestFullTextGlobalIndexer(DataType fieldType, Options options) {
        checkArgument(
                fieldType instanceof VarCharType,
                "TestFullTextGlobalIndexer only supports VARCHAR/STRING, but got: " + fieldType);
        this.fieldType = fieldType;
    }

    @Override
    public GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) throws IOException {
        return new TestFullTextGlobalIndexWriter(fileWriter);
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> files) throws IOException {
        checkArgument(files.size() == 1, "Expected exactly one index file per shard");
        return new TestFullTextGlobalIndexReader(fileReader, files.get(0));
    }
}
