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

package org.apache.paimon.globalindex;

import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.LongUnaryOperator;

/** Abstract base class for global indexers. */
public interface GlobalIndexer {

    GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) throws IOException;

    /**
     * Creates a reader whose relative row IDs cover {@code [0, totalRowCount)}.
     *
     * <p>The complete row count lets an index implementation answer negative predicates by
     * complement, including for an empty shard. Local row ranges are optional pruning hints;
     * callers must still clip the returned candidates.
     */
    GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            long totalRowCount,
            @Nullable List<Range> rowRanges,
            ExecutorService executor);

    /**
     * Rewrites stored index keys with mapped, relative row IDs without reading source data.
     *
     * <p>The input files share the relative row-ID domain {@code [0, sourceRowCount)}. The mapping
     * returns a row ID in {@code [0, targetRowCount)}, or {@code -1} to exclude a source row.
     * Output files share the target domain and may need to be read together, just like the input
     * files.
     */
    default List<ResultEntry> rewriteRowIds(
            GlobalIndexFileReader reader,
            List<GlobalIndexIOMeta> files,
            long sourceRowCount,
            long targetRowCount,
            LongUnaryOperator mapping,
            GlobalIndexFileWriter writer)
            throws IOException {
        throw new UnsupportedOperationException(
                "This global index does not support rewriting stored row IDs.");
    }

    static GlobalIndexer create(String type, List<DataField> indexFields, Options options) {
        return GlobalIndexerFactoryUtils.load(type).create(indexFields, options);
    }
}
