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

package org.apache.paimon.globalindex.testvector;

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;

import java.io.IOException;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A test-only {@link GlobalIndexer} for vector similarity search. Uses brute-force linear scan for
 * ANN queries. No native library dependency required.
 *
 * <p>Supported distance metrics (configured via option {@code test.vector.metric}):
 *
 * <ul>
 *   <li>{@code l2} (default) - Euclidean distance, score = 1 / (1 + distance)
 *   <li>{@code cosine} - Cosine distance, score = 1 - distance
 *   <li>{@code inner_product} - Inner product similarity (directly used as score)
 * </ul>
 */
public class TestVectorGlobalIndexer implements GlobalIndexer {

    /** Option key for vector dimension. */
    public static final String OPT_DIMENSION = "test.vector.dimension";

    /** Option key for distance metric. */
    public static final String OPT_METRIC = "test.vector.metric";

    private final DataType fieldType;
    private final int dimension;
    private final String metric;

    public TestVectorGlobalIndexer(DataType fieldType, Options options) {
        checkArgument(
                fieldType instanceof ArrayType
                        && ((ArrayType) fieldType).getElementType() instanceof FloatType,
                "TestVectorGlobalIndexer only supports ARRAY<FLOAT>, but got: " + fieldType);
        this.fieldType = fieldType;
        this.dimension = options.getInteger(OPT_DIMENSION, 0);
        this.metric = options.getString(OPT_METRIC, "l2");
    }

    @Override
    public GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) throws IOException {
        return new TestVectorGlobalIndexWriter(fileWriter, dimension);
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> files) throws IOException {
        checkArgument(files.size() == 1, "Expected exactly one index file per shard");
        return new TestVectorGlobalIndexReader(fileReader, files.get(0), metric);
    }

    public int dimension() {
        return dimension;
    }

    public String metric() {
        return metric;
    }
}
